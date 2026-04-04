import os
import re
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
    EDITOR_EMPTY_HINT_TEXTS = (
        "发消息",
        "发送消息",
        "發送訊息",
        "發消息",
        "Message",
        "Send a message",
        "信息",
        "Aa",
        "Aa...",
        "Aa…",
    )
    EDITOR_EMPTY_HINT_TEXTS_LOWER = tuple(
        dict.fromkeys(
            " ".join(str(token or "").replace("\xa0", " ").split()).lower()
            for token in EDITOR_EMPTY_HINT_TEXTS
            if str(token or "").strip()
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
        "你无法发消息给这个帐户",
        "你无法发消息给这个账户",
        "你無法發訊息給這個帳戶",
        "你無法發訊息給這個帳號",
        "你无法发消息给这个账号",
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
    SEND_RESTRICTED_VERIFY_IDENTITY_TEXTS = [
        "请验证您的身份以发送消息",
        "请验证你的身份以发送消息",
        "請驗證您的身份以傳送訊息",
        "請驗證你的身份以傳送訊息",
        "Verify your identity to send messages",
        "Confirm your identity to send messages",
        "ยืนยันข้อมูลระบุตัวตนของคุณเพื่อส่งข้อความ",
    ]
    SEND_RESTRICTED_UNUSUAL_ACTIVITY_TEXTS = [
        "由于异常活动，某些操作受到限制",
        "由於異常活動，某些操作受到限制",
        "Due to unusual activity, some actions are restricted",
        "Some actions are restricted due to unusual activity",
        "การดำเนินการบางอย่างถูกจำกัดไว้เนื่องจากมีกิจกรรมที่ผิดปกติ",
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
        *SEND_RESTRICTED_VERIFY_IDENTITY_TEXTS,
        *SEND_RESTRICTED_UNUSUAL_ACTIVITY_TEXTS,
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
    def __init__(self, page, task_cfg=None, window_label=None, runtime_messages=None):
        self.page = page
        self.window_label = (window_label or "").strip()
        task_cfg = task_cfg or {}
        try:
            speed_factor = float(task_cfg.get("speed_factor", 1.0) or 1.0)
        except Exception:
            speed_factor = 1.0
        self.speed_factor = max(0.35, min(1.5, speed_factor))
        self.wait_budget_factor = max(1.0, self.speed_factor)
        self.stranger_limit_cache_ms = int(task_cfg.get("stranger_limit_cache_ms", 1200) or 1200)
        self._last_limit_check_ts = 0.0
        self._last_limit_check_result = False
        self.delivery_confirm_timeout_ms = int(
            task_cfg.get("delivery_confirm_timeout_ms", 2600) or 2600
        )
        self.delivery_recheck_timeout_ms = max(self.delivery_confirm_timeout_ms, 4200)
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
        self.update_runtime_messages(runtime_messages)

    def _normalize_runtime_message_lines(self, values):
        return [
            str(item).strip()
            for item in (values or [])
            if str(item).strip()
        ]

    def _normalize_runtime_messages(self, runtime_messages):
        payload = runtime_messages if isinstance(runtime_messages, dict) else {}
        popup_stop_texts = self._normalize_runtime_message_lines(
            payload.get("popup_stop_texts") or []
        )
        restriction_payload = payload.get("restriction_detection_texts")
        if not popup_stop_texts and isinstance(restriction_payload, dict):
            popup_stop_texts = self._merge_runtime_texts(
                restriction_payload.get("stranger_limit") or [],
                restriction_payload.get("message_request_limit") or [],
            )
        legacy_permanent_skip_texts = []
        if isinstance(restriction_payload, dict):
            legacy_permanent_skip_texts = self._merge_runtime_texts(
                restriction_payload.get("messenger_login_required") or [],
                restriction_payload.get("account_cannot_message") or [],
            )
        return {
            "popup_stop_texts": popup_stop_texts,
            "permanent_skip_texts": self._merge_runtime_texts(
                payload.get("permanent_skip_texts") or [],
                legacy_permanent_skip_texts,
            ),
        }

    def _merge_runtime_texts(self, defaults, extra_texts):
        merged = []
        seen = set()
        for item in list(defaults or []) + list(extra_texts or []):
            normalized = str(item or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
        return merged

    def update_runtime_messages(self, runtime_messages=None):
        normalized = self._normalize_runtime_messages(runtime_messages)
        self.runtime_message_rules = normalized
        self.RUNTIME_POPUP_STOP_TEXTS = self._merge_runtime_texts(
            [],
            normalized.get("popup_stop_texts") or [],
        )
        self.RUNTIME_PERMANENT_SKIP_TEXTS = self._merge_runtime_texts(
            [],
            normalized.get("permanent_skip_texts") or [],
        )
        return normalized

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
        if timeout_ms is None:
            timeout_ms = 300
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        interval = max(0.05, float(interval or 0.05))
        start = time.time()
        deadline = start + timeout_ms / 1000 if timeout_ms > 0 else start
        log.info(f"{self._prefix()}{step_name}，{detail}...")
        last_error = None
        while True:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds
            try:
                result = checker()
            except Exception as exc:
                result = None
                last_error = exc
            if result:
                waited = time.time() - start
                log.info(f"{self._prefix()}{step_name}等待成功，耗时 {waited:.1f} 秒。")
                return result
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(interval, remaining_seconds), min_seconds=0.0)
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
            scaled = int(float(value) * self.wait_budget_factor)
        except Exception:
            scaled = int(value)
        return max(min_ms, scaled)

    def _chat_open_timeout_ms(self, phase="default"):
        total_timeout_ms = self._scale_ms(self.chat_session_wait_timeout_ms, min_ms=1600)
        phase_ratio_map = {
            "initial": 0.80,
            "retry": 0.35,
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
                    const normalize = (value) => String(value || '')
                        .replace(/\\u00a0/g, ' ')
                        .replace(/[\\u200b-\\u200f\\u202a-\\u202e\\u2060-\\u206f\\ufeff]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
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
            entry_cleanup = self._run_surface_cleanup(
                context=f"成员 {name} 入口前置清场",
                close_chat=True,
                dismiss_popup=True,
                dismiss_card=True,
                timeout_ms=self._scale_ms(2200, min_ms=900),
            )
            preclear_chat = entry_cleanup.get("closed_chat", 0)
            preclear_popup = entry_cleanup.get("closed_popup", 0)
            preclear_card = entry_cleanup.get("closed_card", 0)
            preclear_overlay = entry_cleanup.get("overlay_blocking")
            preclear_chat_left = bool(entry_cleanup.get("residual_chat"))
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

            member_card_timeout_ms = self._scale_ms(
                max(1, int(self.hover_card_wait_timeout_ms or 0)),
                min_ms=320,
            )
            member_card_deadline = time.time() + member_card_timeout_ms / 1000

            def remaining_member_card_ms():
                return max(int((member_card_deadline - time.time()) * 1000), 0)

            member_link_state = self._ensure_member_link(
                target,
                name=name,
                timeout_ms=remaining_member_card_ms(),
            )
            link, link_recovered = self._extract_member_link_result(member_link_state)
            if link_recovered:
                log.info(f"{self._prefix()}成员链接恢复后重新命中: {name}")
            if not link:
                log.warning(f"未找到成员列表中的目标链接，当前保留成员待下次重试: {name}")
                return self._build_dm_result("error", "member_link_not_found")
            self._wait_if_paused()
            action_timeout = min(
                self._scale_ms(2000, min_ms=1500),
                remaining_member_card_ms(),
            )
            box = self._safe_bounding_box(
                link,
                timeout_ms=min(
                    self._scale_ms(900, min_ms=250),
                    remaining_member_card_ms(),
                ),
            )
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
                        scroll_timeout_ms = min(action_timeout, remaining_member_card_ms())
                        if scroll_timeout_ms > 0:
                            self._run_pause_aware_action(
                                lambda step_timeout: link.scroll_into_view_if_needed(timeout=step_timeout),
                                timeout_ms=scroll_timeout_ms,
                            )
                    except Exception:
                        refound_link = self._find_member_link(
                            target,
                            timeout_ms=remaining_member_card_ms(),
                        )
                        if refound_link:
                            link = refound_link
                    self._wait_for_stable_box(
                        link,
                        max_rounds=3,
                        delay=0.06,
                        timeout_ms=remaining_member_card_ms(),
                    )
            else:
                try:
                    scroll_timeout_ms = min(action_timeout, remaining_member_card_ms())
                    if scroll_timeout_ms > 0:
                        self._run_pause_aware_action(
                            lambda step_timeout: link.scroll_into_view_if_needed(timeout=step_timeout),
                            timeout_ms=scroll_timeout_ms,
                        )
                except Exception:
                    refound_link = self._find_member_link(
                        target,
                        timeout_ms=remaining_member_card_ms(),
                    )
                    if refound_link:
                        link = refound_link
                self._wait_for_stable_box(
                    link,
                    max_rounds=3,
                    delay=0.06,
                    timeout_ms=remaining_member_card_ms(),
                )
            self._wait_if_paused()

            button_result = self._resolve_member_card_message_button(
                link,
                name,
                timeout_ms=remaining_member_card_ms(),
            )
            button, _, failure, _, _, _, _ = self._extract_member_card_button_result(button_result)
            if failure:
                self._log_restriction_result(failure)
                self._dismiss_member_hover_card()
                return self._restriction_to_dm_result(failure)

            # 这里的正式职责只保留“关闭残留聊天窗口”。
            # 入口前置清场已经处理过 popup；若点击阶段再遇到真实拦截弹层，
            # 由点击失败后的恢复链统一兜底，避免把当前资料卡当成 popup 误关掉。
            self._wait_if_paused()
            preclick_residual_chat = not self._chat_surfaces_cleared()
            if preclick_residual_chat:
                self._run_surface_cleanup(
                    context=f"成员 {name} 发消息前清场",
                    close_chat=True,
                    dismiss_popup=False,
                    dismiss_card=False,
                    timeout_ms=self._scale_ms(1200, min_ms=480),
                )

            click_result = self._click_member_card_message_button(
                button,
                link=link,
                target=target,
                name=name,
                timeout_ms=min(action_timeout, remaining_member_card_ms()),
            )
            clicked_button, _, click_failure, _, _, _, _ = self._extract_member_card_button_result(click_result)
            if not clicked_button:
                click_failure = self._classify_message_button_click_failure(
                    name=name,
                    restriction=click_failure,
                )
                self._log_restriction_result(click_failure)
                self._dismiss_interfering_popups(context=f"成员 {name} 限制对话框收尾")
                self._close_visible_chat_windows()
                self._dismiss_member_hover_card()
                return self._restriction_to_dm_result(click_failure)
            self._wait_if_paused()

            prechat_result = self._prepare_prechat_send_session(
                button=button,
                link=link,
                target=target,
                name=name,
                expected_name=name,
                open_timeout_ms=self._chat_open_timeout_ms("default"),
                action_timeout_ms=action_timeout,
                ready_phase="default",
            )
            chat_session, prechat_dm_result = self._extract_prechat_send_session_result(prechat_result)
            if prechat_dm_result:
                return prechat_dm_result

            delivery_state = "timeout"
            close_delivery_state = "timeout"
            closed_chat = 0
            closed_card = 0
            try:
                primary_text = self._normalize_text(greeting_text) or self._normalize_text(probe_text)
                if not primary_text:
                    return self._build_dm_result("error", "empty_message")

                delivery_result = self._paste_and_send(
                    chat_session,
                    primary_text,
                    expected_name=name,
                )
                delivery_state, rebound_session = self._extract_delivery_session_result(
                    delivery_result
                )
                chat_session = rebound_session
                delivery_result = self._resolve_delivery_state_dm_result(
                    delivery_state,
                    name=name,
                    stage="postsend",
                    paste_failed_log=f"{self._prefix()}聊天输入框粘贴消息失败，当前保留成员待下次重试: {name}",
                    editor_not_ready_log=f"{self._prefix()}聊天输入框未准备就绪，当前保留成员待下次重试: {name}",
                    send_restricted_alert_text="聊天窗口明确提示发送失败，当前窗口停止继续发送，请更换账户。"
                    if delivery_state == "send_restricted_ui"
                    else None,
                )
                if delivery_result:
                    return delivery_result

                if delivery_state == "sent":
                    close_delivery_state = "sent"
                    log.success(f"✅ 成员页消息已发送: {name}")
                    return self._build_dm_result("sent", "sent")

                if enable_delivery_probe and self._normalize_text(probe_text):
                    log.info(f"{self._prefix()}正文未确认，开始发送点赞型探针: {name}")
                    probe_result_state = self._run_delivery_probe(
                        chat_session,
                        probe_text,
                        expected_name=name,
                        prior_text=primary_text,
                    )
                    probe_state, probe_session = self._extract_delivery_session_result(
                        probe_result_state
                    )
                    chat_session = probe_session

                    probe_result = self._resolve_delivery_state_dm_result(
                        probe_state,
                        name=name,
                        stage="probe",
                        paste_failed_log=(
                            f"{self._prefix()}点赞型探针输入框粘贴消息失败，当前保留成员待下次重试: {name}"
                        ),
                        editor_not_ready_log=(
                            f"{self._prefix()}点赞型探针输入框未准备就绪，当前保留成员待下次重试: {name}"
                        ),
                        send_restricted_alert_text="点赞探针发送后明确提示当前无法发送，当前窗口停止继续发送，请更换账户。"
                        if probe_state == "send_restricted_ui"
                        else None,
                    )
                    if probe_result:
                        return probe_result

                    if probe_state == "confirmed_sent":
                        close_delivery_state = "sent"
                        log.success(f"{self._prefix()}正文延迟确认成功，无需继续发送点赞型探针: {name}")
                        return self._build_dm_result("sent", "sent")

                    if probe_state == "sent":
                        close_delivery_state = "sent"
                        log.success(f"{self._prefix()}点赞型探针发送成功，当前窗口恢复正常: {name}")
                        return self._build_dm_result("skip", "delivery_probe_sent")

                    log.warning(f"{self._prefix()}点赞型探针未确认成功，保留静默失败计数: {name}")

                log.warning(f"发送消息在两轮确认后仍未命中，当前按未确认处理: {name}")
                return self._build_dm_result("error", "delivery_not_confirmed")
            finally:
                close_delay_ms = (
                    self.success_close_delay_ms if close_delivery_state == "sent" else self.default_close_delay_ms
                )
                self._sleep(max(close_delay_ms, 0) / 1000, min_seconds=0.05)
                close_wait_ms = self._scale_ms(2600 if close_delivery_state == "sent" else 1200, min_ms=480)
                close_session = self._resolve_close_chat_session(
                    chat_session,
                    expected_name=name,
                    timeout_ms=close_wait_ms,
                )
                close_editor, close_button = self._extract_chat_session(close_session)
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

    def _run_delivery_probe(self, chat_session, probe_text, expected_name=None, prior_text=None):
        normalized_probe = self._normalize_text(probe_text)
        normalized_prior_text = self._normalize_text(prior_text)

        probe_precheck_timeout_ms = self._postsend_timeout_resolve_budget_ms()
        probe_recovery_timeout_ms = (
            self._chat_open_timeout_ms("probe")
            + self._editor_ready_timeout_ms("probe")
            + probe_precheck_timeout_ms
        )
        preprobe_result = self._retry_delivery_confirmation(
            chat_session=chat_session,
            text=normalized_prior_text or None,
            before_state=None,
            expected_name=expected_name,
            timeout_ms=probe_recovery_timeout_ms,
            log_text="",
            open_phase="probe",
            stage="点赞探针前复核",
            recovery_stage="点赞探针前清理后复核",
            ready_stage="点赞探针输入框恢复复核",
            ready_phase="probe",
            recovered_log_text=(
                f"{self._prefix()}点赞探针输入框清理恢复后重新就绪: "
                f"{expected_name or 'unknown'}"
            ),
        )
        preprobe_state, probe_session = self._extract_delivery_session_result(preprobe_result)
        if preprobe_state != "timeout":
            return self._build_probe_precheck_result(
                state=preprobe_state,
                chat_session=probe_session,
            )

        probe_result = self._paste_and_send(
            probe_session,
            normalized_probe,
            expected_name=expected_name,
        )
        return probe_result

    def _wait_for_chat_session_text(self, chat_session, text, expected_name=None, timeout_ms=None):
        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return None
        if timeout_ms is None:
            timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 18), min_ms=900)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            if timeout_ms <= 0:
                return None
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

            refreshed_session = self._refresh_chat_session(
                current_session,
                expected_name=expected_name,
                timeout_ms=max(int((deadline - time.time()) * 1000), 0),
                allow_generic=False,
            )
            if refreshed_session:
                current_session = refreshed_session
                refreshed_editor, _ = self._extract_chat_session(refreshed_session)
                if self._editor_contains_text(refreshed_editor, text):
                    return current_session

            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)
        return None

    def _paste_text_into_chat_session(self, chat_session, text, expected_name=None, timeout_ms=None):
        current_session = chat_session

        normalized_text = self._normalize_text(text)
        if not normalized_text:
            return self._build_paste_chat_session_result(
                state="paste_failed",
                chat_session=chat_session,
            )
        editor, close_button = self._extract_chat_session(current_session)
        current_name = expected_name or "unknown"

        def ensure_paste_ready(stage):
            nonlocal current_session, editor, close_button
            ready_state = self._ensure_ready_chat_session(
                name=current_name,
                chat_session=current_session,
                expected_name=expected_name,
                stage=stage,
                timeout_ms=remaining_stage_ms(),
                ready_phase="default",
            )
            (
                updated_session,
                _,
                restriction_state,
                _,
                ready_available,
                cleanup_recovered,
            ) = self._extract_ready_chat_session_result(ready_state)
            current_session = updated_session
            editor, close_button = self._extract_chat_session(current_session)
            if restriction_state:
                return self._build_paste_chat_session_result(
                    state=restriction_state,
                    chat_session=current_session,
                )
            if ready_available:
                if cleanup_recovered:
                    log.info(f"{self._prefix()}聊天输入框粘贴恢复后重新就绪: {current_name}")
                return self._build_paste_chat_session_result(
                    state="ready",
                    chat_session=current_session,
                )
            return self._build_paste_chat_session_result(
                state="chat_editor_not_ready",
                chat_session=current_session,
            )

        is_macos = (
            os.name != "nt"
            and getattr(os, "uname", None) is not None
            and os.uname().sysname == "Darwin"
        )
        select_all_shortcut = "Meta+A" if is_macos else "Control+A"
        paste_shortcut = "Meta+V" if is_macos else "Control+V"
        focus_timeout_ms = self._scale_ms(1200, min_ms=600)
        shortcut_timeout_ms = self._scale_ms(600, min_ms=240)
        clear_timeout_ms = self._scale_ms(900, min_ms=420)
        copy_timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 18), min_ms=900)
        paste_timeout_ms = self._scale_ms(1200, min_ms=720)
        pasted_wait_timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 20), min_ms=900)
        fill_timeout_ms = self._scale_ms(max(1200, len(normalized_text) * 24), min_ms=900)
        stage_timeout_ms = (
            focus_timeout_ms
            + shortcut_timeout_ms
            + shortcut_timeout_ms
            + clear_timeout_ms
            + max(copy_timeout_ms + paste_timeout_ms + pasted_wait_timeout_ms, fill_timeout_ms * 2)
            + self._scale_ms(300, min_ms=120)
        )
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            if timeout_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=chat_session,
                )
            stage_timeout_ms = min(stage_timeout_ms, timeout_ms)
        deadline = time.time() + stage_timeout_ms / 1000

        def remaining_stage_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        if not editor:
            ready_result = ensure_paste_ready("聊天输入框粘贴前恢复复核")
            if ready_result["state"] != "ready":
                return ready_result

        try:
            step_timeout_ms = min(remaining_stage_ms(), focus_timeout_ms)
            if step_timeout_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._run_pause_aware_action(
                lambda step_timeout: editor.click(timeout=step_timeout),
                timeout_ms=step_timeout_ms,
            )
        except Exception:
            ready_result = ensure_paste_ready("聊天输入框点击恢复复核")
            if ready_result["state"] != "ready":
                return ready_result
            try:
                step_timeout_ms = min(remaining_stage_ms(), focus_timeout_ms)
                if step_timeout_ms <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                self._run_pause_aware_action(
                    lambda step_timeout: editor.click(timeout=step_timeout),
                    timeout_ms=step_timeout_ms,
                )
            except Exception:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
        remaining_seconds = max(deadline - time.time(), 0.0)
        if remaining_seconds <= 0:
            return self._build_paste_chat_session_result(
                state="paste_failed",
                chat_session=current_session,
            )
        self._sleep(min(0.04, remaining_seconds), min_seconds=0.0)
        try:
            step_timeout_ms = min(remaining_stage_ms(), shortcut_timeout_ms)
            if step_timeout_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._run_pause_aware_action(
                lambda step_timeout: editor.press(
                    select_all_shortcut,
                    timeout=step_timeout,
                ),
                timeout_ms=step_timeout_ms,
                slice_ms=180,
            )
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._sleep(min(0.02, remaining_seconds), min_seconds=0.0)
            step_timeout_ms = min(remaining_stage_ms(), shortcut_timeout_ms)
            if step_timeout_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._run_pause_aware_action(
                lambda step_timeout: editor.press("Backspace", timeout=step_timeout),
                timeout_ms=step_timeout_ms,
                slice_ms=180,
            )
        except Exception:
            pass
        remaining_seconds = max(deadline - time.time(), 0.0)
        if remaining_seconds <= 0:
            return self._build_paste_chat_session_result(
                state="paste_failed",
                chat_session=current_session,
            )
        self._sleep(min(0.04, remaining_seconds), min_seconds=0.0)
        if not self._editor_empty(editor):
            try:
                step_timeout_ms = min(remaining_stage_ms(), clear_timeout_ms)
                if step_timeout_ms <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                self._run_pause_aware_action(
                    lambda step_timeout: editor.fill("", timeout=step_timeout),
                    timeout_ms=step_timeout_ms,
                    slice_ms=step_timeout_ms,
                )
            except Exception:
                pass
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._sleep(min(0.04, remaining_seconds), min_seconds=0.0)
        if not self._editor_empty(editor):
            ready_result = ensure_paste_ready("聊天输入框清空恢复复核")
            if ready_result["state"] != "ready":
                return ready_result
            try:
                step_timeout_ms = min(remaining_stage_ms(), clear_timeout_ms)
                if step_timeout_ms <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                self._run_pause_aware_action(
                    lambda step_timeout: editor.fill("", timeout=step_timeout),
                    timeout_ms=step_timeout_ms,
                    slice_ms=step_timeout_ms,
                )
            except Exception:
                pass
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._sleep(min(0.04, remaining_seconds), min_seconds=0.0)
            if not self._editor_empty(editor):
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
        try:
            clipboard_timeout_ms = min(remaining_stage_ms(), copy_timeout_ms)
            if clipboard_timeout_ms > 0 and self._write_text_to_clipboard(text, timeout_ms=clipboard_timeout_ms):
                step_timeout_ms = min(remaining_stage_ms(), paste_timeout_ms)
                if step_timeout_ms <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                self._run_pause_aware_action(
                    lambda step_timeout: editor.press(paste_shortcut, timeout=step_timeout),
                    timeout_ms=step_timeout_ms,
                    slice_ms=step_timeout_ms,
                )
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)
                pasted_wait_ms = min(remaining_stage_ms(), pasted_wait_timeout_ms)
                if pasted_wait_ms <= 0:
                    return self._build_paste_chat_session_result(
                        state="paste_failed",
                        chat_session=current_session,
                    )
                pasted_session = self._wait_for_chat_session_text(
                    current_session,
                    text,
                    expected_name=expected_name,
                    timeout_ms=pasted_wait_ms,
                )
                if pasted_session:
                    return self._build_paste_chat_session_result(
                        state="ready",
                        chat_session=pasted_session,
                    )
        except Exception:
            pass
        try:
            if not editor or self._chat_editor_gone(editor, timeout_ms=remaining_stage_ms()):
                ready_result = ensure_paste_ready("聊天输入框填充前恢复复核")
                if ready_result["state"] != "ready":
                    return ready_result
            if not editor:
                return self._build_paste_chat_session_result(
                    state="chat_editor_not_ready",
                    chat_session=current_session,
                )
            step_timeout_ms = min(remaining_stage_ms(), fill_timeout_ms)
            if step_timeout_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._run_pause_aware_action(
                lambda step_timeout: editor.fill(text, timeout=step_timeout),
                timeout_ms=step_timeout_ms,
                slice_ms=step_timeout_ms,
            )
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            self._sleep(min(0.05, remaining_seconds), min_seconds=0.0)
            fill_wait_ms = min(remaining_stage_ms(), fill_timeout_ms)
            if fill_wait_ms <= 0:
                return self._build_paste_chat_session_result(
                    state="paste_failed",
                    chat_session=current_session,
                )
            filled_session = self._wait_for_chat_session_text(
                current_session,
                text,
                expected_name=expected_name,
                timeout_ms=fill_wait_ms,
            )
            if filled_session:
                return self._build_paste_chat_session_result(
                    state="ready",
                    chat_session=filled_session,
                )
            return self._build_paste_chat_session_result(
                state="paste_failed",
                chat_session=current_session,
            )
        except Exception:
            return self._build_paste_chat_session_result(
                state="paste_failed",
                chat_session=current_session,
            )

    def _write_text_to_clipboard(self, text, timeout_ms=None):
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
            if timeout_ms is not None:
                try:
                    timeout_ms = int(timeout_ms)
                except Exception:
                    timeout_ms = 0
                copy_timeout_ms = min(copy_timeout_ms, max(timeout_ms, 0))
            if copy_timeout_ms <= 0:
                return False
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

    def _map_delivery_state_to_restriction_reason(self, delivery_state):
        normalized_state = str(delivery_state or "").strip()
        if not normalized_state:
            return ""
        if normalized_state == "limit_reached":
            return "stranger_message_limit_reached"
        if normalized_state == "message_request_limit_reached":
            return "message_request_limit_reached"
        if self._is_skip_restriction_reason(normalized_state):
            return normalized_state
        if normalized_state == "send_restricted_ui":
            return "send_restricted_ui"
        return ""

    def _resolve_delivery_state_dm_result(
        self,
        delivery_state,
        name="",
        stage="",
        paste_failed_log="",
        editor_not_ready_log="",
        send_restricted_alert_text=None,
    ):
        if delivery_state == "paste_failed":
            if paste_failed_log:
                log.warning(paste_failed_log)
            return self._build_dm_result("error", "chat_editor_paste_failed")

        if delivery_state == "chat_editor_not_ready":
            if editor_not_ready_log:
                log.warning(editor_not_ready_log)
            return self._build_dm_result("error", "chat_editor_not_ready")

        delivery_restriction = self._build_restriction_result(
            self._map_delivery_state_to_restriction_reason(delivery_state),
            name=name,
            stage=stage,
            alert_text=send_restricted_alert_text,
        )
        if delivery_restriction:
            self._log_restriction_result(delivery_restriction)
            return self._restriction_to_dm_result(delivery_restriction)

        return None

    def _check_postsend_delivery_restriction(self, expected_name=None):
        if self._check_stranger_limit_global():
            return "limit_reached"
        restriction = self._detect_chat_restriction(
            expected_name=expected_name,
            stage="postsend",
        )
        return self._map_postsend_restriction_to_delivery_state(restriction)

    def _target_chat_session_closed(
        self,
        chat_session=None,
        expected_name=None,
        timeout_ms=None,
        refreshed_session=None,
    ):
        editor, _ = self._extract_chat_session(chat_session)
        if not editor:
            return False
        if timeout_ms is None:
            check_timeout_ms = self._scale_ms(220, min_ms=80)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                check_timeout_ms = 1
            else:
                check_timeout_ms = min(self._scale_ms(220, min_ms=80), timeout_ms)
        if not self._chat_editor_gone(editor, timeout_ms=check_timeout_ms):
            return False
        if not expected_name:
            return self._chat_window_closed(editor)

        active_session = refreshed_session
        if not active_session:
            active_session = self._refresh_chat_session(
                chat_session,
                expected_name=expected_name,
                timeout_ms=check_timeout_ms,
                allow_generic=False,
            )
        if not active_session:
            return True

        active_editor, _ = self._extract_chat_session(active_session)
        return self._chat_editor_gone(active_editor, timeout_ms=check_timeout_ms)

    def _resolve_postsend_immediate_state(
        self,
        chat_session=None,
        expected_name=None,
        timeout_ms=None,
    ):
        postsend_restriction = self._check_postsend_delivery_restriction(
            expected_name=expected_name,
        )
        if postsend_restriction:
            return postsend_restriction
        if chat_session and self._target_chat_session_closed(
            chat_session,
            expected_name=expected_name,
            timeout_ms=timeout_ms,
        ):
            return "sent"
        return None

    def _postsend_timeout_resolve_budget_ms(self):
        return min(
            self.delivery_recheck_timeout_ms,
            self._scale_ms(max(self.delivery_settle_ms + 300, 900), min_ms=480),
        )

    def _resolve_postsend_timeout_result(
        self,
        expected_name=None,
        session=None,
        text=None,
        before_state=None,
        timeout_ms=None,
    ):
        if timeout_ms is None:
            timeout_ms = self._postsend_timeout_resolve_budget_ms()
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)

        deadline = time.time() + timeout_ms / 1000

        def remaining_timeout_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        current_session = session
        immediate_state = self._resolve_postsend_immediate_state(
            chat_session=current_session,
            expected_name=expected_name,
            timeout_ms=remaining_timeout_ms(),
        )
        if immediate_state:
            return self._build_delivery_session_result(
                state=immediate_state,
                chat_session=current_session,
            )

        if timeout_ms > 0:
            recovered_session_state = self._ensure_postsend_chat_session(
                chat_session=current_session,
                expected_name=expected_name,
                open_phase="postsend_retry",
                stage="发送后终态复核",
                recovery_stage="发送后终态清理后复核",
                timeout_ms=remaining_timeout_ms(),
            )
            recovered_session, recovered_restriction = self._extract_postsend_chat_session_result(
                recovered_session_state
            )
            if recovered_restriction:
                return self._build_delivery_session_result(
                    state=recovered_restriction,
                    chat_session=recovered_session,
                )
            current_session = recovered_session

        final_wait_timeout_ms = remaining_timeout_ms()
        if (
            text is not None
            and current_session
            and final_wait_timeout_ms > 0
        ):
            final_result = self._wait_for_delivery(
                current_session,
                text,
                before_state,
                expected_name=expected_name,
                timeout_ms=final_wait_timeout_ms,
            )
            final_state, final_session = self._extract_delivery_session_result(final_result)
            current_session = final_session
            if final_state != "timeout":
                return final_result

        final_restriction = self._check_postsend_delivery_restriction(
            expected_name=expected_name,
        )
        return self._build_delivery_session_result(
            state=final_restriction or "timeout",
            chat_session=current_session,
        )

    def _ensure_postsend_chat_session(
        self,
        chat_session=None,
        expected_name=None,
        open_phase="probe",
        stage="",
        recovery_stage="",
        timeout_ms=None,
    ):
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return self._build_postsend_chat_session_result()
            deadline = time.time() + timeout_ms / 1000

        def remaining_postsend_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        current_session = None
        editor, _ = self._extract_chat_session(chat_session)
        if chat_session and not self._chat_editor_gone(editor):
            refreshed_session = self._refresh_chat_session(
                chat_session,
                expected_name=expected_name,
                timeout_ms=remaining_postsend_ms(),
                allow_generic=False,
            )
            current_session = refreshed_session
        if current_session:
            return self._build_postsend_chat_session_result(chat_session=current_session)

        open_timeout_ms = self._chat_open_timeout_ms(open_phase)
        if deadline is not None:
            open_timeout_ms = min(open_timeout_ms, remaining_postsend_ms())
            if open_timeout_ms <= 0:
                return self._build_postsend_chat_session_result()
        open_result = self._wait_for_chat_open_result(
            expected_name=expected_name,
            timeout_ms=open_timeout_ms,
            stage=stage,
        )
        current_session, _, delivery_state, shell_seen, _ = self._extract_chat_open_result(
            open_result
        )
        if delivery_state:
            return self._build_postsend_chat_session_result(state=delivery_state)

        if not current_session:
            recovered_session = None
            if (shell_seen or self._detect_blocking_overlay()) and (
                remaining_postsend_ms() is None or remaining_postsend_ms() > 0
            ):
                recovered_open_result = self._recover_chat_open_after_surface_cleanup(
                    name=expected_name or "unknown",
                    expected_name=expected_name,
                    stage=recovery_stage or stage,
                    timeout_ms=remaining_postsend_ms(),
                )
                recovered_session, _, delivery_state, _, _ = self._extract_chat_open_result(
                    recovered_open_result
                )
                if delivery_state:
                    return self._build_postsend_chat_session_result(state=delivery_state)
                current_session = recovered_session
                if current_session:
                    log.info(
                        f"{self._prefix()}{recovery_stage or stage or '发送后聊天窗口'}清理恢复后重新可用: "
                        f"{expected_name or 'unknown'}"
                    )

        return self._build_postsend_chat_session_result(chat_session=current_session)

    def _prepare_postsend_ready_session(
        self,
        chat_session=None,
        expected_name=None,
        open_phase="postsend_retry",
        stage="",
        recovery_stage="",
        ready_stage="",
        ready_phase="postsend_retry",
        timeout_ms=None,
    ):
        deadline = None

        def remaining_prepare_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            deadline = time.time() + timeout_ms / 1000

        immediate_state = self._resolve_postsend_immediate_state(
            chat_session=chat_session,
            expected_name=expected_name,
            timeout_ms=remaining_prepare_ms(),
        )
        if immediate_state:
            return self._build_postsend_ready_session_result(
                state=immediate_state,
                chat_session=chat_session,
            )

        prepared_session_state = self._ensure_postsend_chat_session(
            chat_session=chat_session,
            expected_name=expected_name,
            open_phase=open_phase,
            stage=stage,
            recovery_stage=recovery_stage,
            timeout_ms=remaining_prepare_ms(),
        )
        prepared_session, prepared_restriction = self._extract_postsend_chat_session_result(
            prepared_session_state
        )
        if prepared_restriction:
            return self._build_postsend_ready_session_result(state=prepared_restriction)
        if not prepared_session:
            return self._build_postsend_ready_session_result(
                state="timeout",
            )

        ready_state = self._ensure_ready_chat_session(
            name=expected_name or "unknown",
            chat_session=prepared_session,
            expected_name=expected_name,
            stage=ready_stage,
            timeout_ms=remaining_prepare_ms(),
            ready_phase=ready_phase,
        )
        (
            ready_session,
            _,
            recovery_state,
            _,
            ready_recovered,
            cleanup_recovered,
        ) = self._extract_ready_chat_session_result(ready_state)
        if recovery_state:
            return self._build_postsend_ready_session_result(state=recovery_state)
        if ready_recovered:
            return self._build_postsend_ready_session_result(
                state="ready",
                chat_session=ready_session,
                recovered=cleanup_recovered,
            )
        return self._build_postsend_ready_session_result(
            state="timeout",
            chat_session=ready_session,
        )

    def _retry_delivery_confirmation(
        self,
        chat_session,
        text,
        before_state,
        expected_name=None,
        timeout_ms=None,
        log_text=None,
        open_phase="postsend_retry",
        stage="发送后二次确认复核",
        recovery_stage="发送后二次确认清理后复核",
        ready_stage="发送后二次确认输入框恢复复核",
        ready_phase="postsend_retry",
        recovered_log_text=None,
    ):
        if log_text is None:
            log_text = (
                f"{self._prefix()}发送确认首轮未命中，保留当前聊天窗口并进行二次核对: "
                f"{expected_name or 'unknown'}"
            )
        if log_text:
            log.info(log_text)
        if timeout_ms is None:
            timeout_ms = self.delivery_recheck_timeout_ms
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        retry_deadline = time.time() + timeout_ms / 1000

        def remaining_retry_ms():
            return max(int((retry_deadline - time.time()) * 1000), 0)

        prepared_rebound = self._prepare_postsend_ready_session(
            chat_session=chat_session,
            expected_name=expected_name,
            open_phase=open_phase,
            stage=stage,
            recovery_stage=recovery_stage,
            ready_stage=ready_stage,
            ready_phase=ready_phase,
            timeout_ms=remaining_retry_ms(),
        )
        (
            rebound_prepare_state,
            rebound_session,
            rebound_recovered,
        ) = self._extract_postsend_ready_session_result(prepared_rebound)
        if rebound_prepare_state != "ready":
            if rebound_prepare_state != "timeout":
                return self._build_delivery_session_result(
                    state=rebound_prepare_state,
                    chat_session=rebound_session,
                )
        else:
            if rebound_recovered:
                if recovered_log_text is None:
                    recovered_log_text = (
                        f"{self._prefix()}发送后二次确认输入框清理恢复后重新就绪: "
                        f"{expected_name or 'unknown'}"
                    )
                if recovered_log_text:
                    log.info(recovered_log_text)

            second_pass_result = self._wait_for_delivery(
                rebound_session,
                text,
                before_state,
                expected_name=expected_name,
                timeout_ms=remaining_retry_ms(),
            )
            second_pass_state, delivery_session = self._extract_delivery_session_result(
                second_pass_result
            )
            rebound_session = delivery_session
            if second_pass_state != "timeout":
                return second_pass_result

        return self._resolve_postsend_timeout_result(
            expected_name=expected_name,
            session=rebound_session,
            text=text,
            before_state=before_state,
            timeout_ms=remaining_retry_ms(),
        )

    def _paste_and_send(self, chat_session, text, expected_name=None):
        active_session = chat_session

        editor, _ = self._extract_chat_session(chat_session)
        before_state = self._normalize_delivery_state(
            self._get_delivery_state(editor, text) if editor else None
        )
        paste_result = self._paste_text_into_chat_session(
            chat_session,
            text,
            expected_name=expected_name,
        )
        paste_state, pasted_session = self._extract_paste_chat_session_result(paste_result)
        active_session = pasted_session
        if paste_state != "ready":
            return self._build_delivery_session_result(
                state=paste_state or "paste_failed",
                chat_session=active_session,
            )
        editor, _ = self._extract_chat_session(active_session)
        self._sleep(0.04, min_seconds=0.01)
        try:
            enter_timeout_ms = self._scale_ms(900, min_ms=360)
            self._run_pause_aware_action(
                lambda step_timeout: editor.press("Enter", timeout=step_timeout),
                timeout_ms=enter_timeout_ms,
                slice_ms=enter_timeout_ms,
            )
        except Exception:
            return self._build_delivery_session_result(
                state="chat_editor_not_ready",
                chat_session=active_session,
            )
        delivery_result = self._wait_for_delivery(
            active_session,
            text,
            before_state,
            expected_name=expected_name,
            timeout_ms=self.delivery_confirm_timeout_ms,
        )
        delivery_state, delivery_session = self._extract_delivery_session_result(
            delivery_result
        )
        active_session = delivery_session
        if delivery_state != "timeout":
            return delivery_result
        retry_result = self._retry_delivery_confirmation(
            active_session,
            text,
            before_state,
            expected_name=expected_name,
        )
        return retry_result

    def _wait_for_delivery(self, chat_session, text, before_state, expected_name=None, timeout_ms=None):
        if timeout_ms is None:
            timeout_ms = self.delivery_confirm_timeout_ms
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        before_state = self._normalize_delivery_state(before_state)
        before_hits = before_state["message_hits"]
        before_fingerprint = before_state["window_fingerprint"]
        fast_settle_ms = min(self.delivery_settle_ms, 350)
        commit_settle_ms = min(self.delivery_settle_ms, 220)
        deadline = time.time() + timeout_ms / 1000
        success_seen_at = None
        success_mode = None
        current_session = chat_session
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                deadline += paused_seconds
                if success_seen_at is not None:
                    success_seen_at += paused_seconds

            editor, close_button = self._extract_chat_session(current_session)
            refreshed_session = self._refresh_chat_session(
                current_session,
                expected_name=expected_name,
                timeout_ms=max(int((deadline - time.time()) * 1000), 0),
                allow_generic=False,
            )
            if refreshed_session:
                refreshed_editor, refreshed_close_button = self._extract_chat_session(refreshed_session)
                current_session = refreshed_session
                editor = refreshed_editor
                close_button = refreshed_close_button

            if self._target_chat_session_closed(
                current_session,
                expected_name=expected_name,
                timeout_ms=max(int((deadline - time.time()) * 1000), 0),
                refreshed_session=refreshed_session,
            ):
                current_mode = "target_session_closed_commit"
                if success_seen_at is None or success_mode != current_mode:
                    success_seen_at = time.time()
                    success_mode = current_mode
                elif (time.time() - success_seen_at) * 1000 >= commit_settle_ms:
                    return self._build_delivery_session_result(
                        state="sent",
                        chat_session=current_session,
                    )
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    break
                self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)
                continue

            editor_empty = self._editor_empty(editor)
            state = self._normalize_delivery_state(self._get_delivery_state(editor, text))
            postsend_state = self._check_postsend_delivery_restriction(
                expected_name=expected_name,
            )
            if postsend_state:
                return self._build_delivery_session_result(
                    state=postsend_state,
                    chat_session=current_session,
                )
            if not state["state_ready"]:
                success_seen_at = None
                success_mode = None
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    break
                self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)
                continue

            fingerprint_changed = (
                bool(before_fingerprint)
                and bool(state["window_fingerprint"])
                and state["window_fingerprint"] != before_fingerprint
            )

            if state["has_status"] or state["has_sent_time"]:
                return self._build_delivery_session_result(
                    state="sent",
                    chat_session=current_session,
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
                    return self._build_delivery_session_result(
                        state="sent",
                        chat_session=current_session,
                    )
            elif editor_empty and fingerprint_changed:
                current_mode = "editor_commit"
                if success_seen_at is None or success_mode != current_mode:
                    success_seen_at = time.time()
                    success_mode = current_mode
                elif (time.time() - success_seen_at) * 1000 >= commit_settle_ms:
                    return self._build_delivery_session_result(
                        state="sent",
                        chat_session=current_session,
                    )
            else:
                success_seen_at = None
                success_mode = None
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)
        return self._build_delivery_session_result(
            state="timeout",
            chat_session=current_session,
        )

    def _build_delivery_state(self, state_ready=False, state=None):
        payload = state if isinstance(state, dict) else {}
        return {
            "state_ready": bool(state_ready),
            "message_hits": int(payload.get("message_hits") or 0),
            "has_status": bool(payload.get("has_status")),
            "has_sent_time": bool(payload.get("has_sent_time")),
            "window_fingerprint": self._normalize_text(payload.get("window_fingerprint") or ""),
        }

    def _normalize_delivery_state(self, state):
        if not isinstance(state, dict):
            return self._build_delivery_state(state_ready=False)
        return self._build_delivery_state(state_ready=True, state=state)

    def _editor_empty(self, editor):
        try:
            payload = editor.evaluate(
                """(el) => {
                    if (!el) {
                        return {
                            content: '',
                            ariaLabel: '',
                            placeholder: '',
                            ariaPlaceholder: ''
                        };
                    }
                    const readContent = () => {
                        if (el.isContentEditable || el.getAttribute('contenteditable') === 'true') {
                            return (el.innerText || el.textContent || '');
                        }
                        if ('value' in el) {
                            return el.value || '';
                        }
                        return el.textContent || '';
                    };
                    return {
                        content: readContent(),
                        ariaLabel: (el.getAttribute && el.getAttribute('aria-label')) || '',
                        placeholder: (
                            (el.getAttribute && el.getAttribute('placeholder'))
                            || (el.getAttribute && el.getAttribute('data-placeholder'))
                            || ''
                        ),
                        ariaPlaceholder: (el.getAttribute && el.getAttribute('aria-placeholder')) || ''
                    };
                }"""
            )
            if not isinstance(payload, dict):
                return not self._normalize_text(payload)
            content = self._normalize_text(payload.get("content") or "")
            if not content:
                return True
            content_lower = content.lower()
            hint_texts = set(self.EDITOR_EMPTY_HINT_TEXTS_LOWER)
            for field in ("ariaLabel", "placeholder", "ariaPlaceholder"):
                normalized_hint = self._normalize_text(payload.get(field) or "").lower()
                if normalized_hint:
                    hint_texts.add(normalized_hint)
            return content_lower in hint_texts
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
                        const normalize = (value) => String(value || '')
                            .replace(/\\u00a0/g, ' ')
                            .replace(/[\\u200b-\\u200f\\u202a-\\u202e\\u2060-\\u206f\\ufeff]/g, '')
                            .replace(/\\s+/g, ' ')
                            .trim();
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
            "sendRestrictedVerifyIdentityTexts": self.SEND_RESTRICTED_VERIFY_IDENTITY_TEXTS,
            "sendRestrictedUnusualActivityTexts": self.SEND_RESTRICTED_UNUSUAL_ACTIVITY_TEXTS,
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
                            const matchesIdentityVerificationSendRestriction = () => {
                                if ((payload.sendRestrictedVerifyIdentityTexts || []).some((token) => text.includes(token))) {
                                    return true;
                                }
                                if ((payload.sendRestrictedUnusualActivityTexts || []).some((token) => text.includes(token))) {
                                    return true;
                                }
                                if (
                                    (
                                        (lowerText.includes('verify your identity') || lowerText.includes('confirm your identity'))
                                        && (lowerText.includes('send message') || lowerText.includes('send messages'))
                                    )
                                    || (
                                        text.includes('验证')
                                        && text.includes('身份')
                                        && (text.includes('发送消息') || text.includes('傳送訊息'))
                                    )
                                    || (
                                        lowerText.includes('unusual activity')
                                        && (lowerText.includes('restricted') || lowerText.includes('limit'))
                                    )
                                    || (
                                        text.includes('异常活动')
                                        && (text.includes('受到限制') || text.includes('被限制'))
                                    )
                                    || (
                                        text.includes('異常活動')
                                        && (text.includes('受到限制') || text.includes('被限制'))
                                    )
                                    || (
                                        text.includes('ยืนยันข้อมูลระบุตัวตน')
                                        && text.includes('ส่งข้อความ')
                                    )
                                    || (
                                        text.includes('กิจกรรมที่ผิดปกติ')
                                        && text.includes('ถูกจำกัด')
                                    )
                                ) {
                                    return true;
                                }
                                return false;
                            };
                            if ((payload.strangerLimitTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.messageRequestLimitTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.messengerLoginRequiredTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.accountCannotMessageTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.failureTexts || []).some((token) => text.includes(token))) return true;
                            if ((payload.sendRestrictedTexts || []).some((token) => text.includes(token))) return true;
                            if (matchesIdentityVerificationSendRestriction()) return true;
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
                            const matchesIdentityVerificationSendRestriction = () => {
                                if ((payload.sendRestrictedVerifyIdentityTexts || []).some((token) => text.includes(token))) {
                                    return true;
                                }
                                if ((payload.sendRestrictedUnusualActivityTexts || []).some((token) => text.includes(token))) {
                                    return true;
                                }
                                if (
                                    (
                                        (lowerText.includes('verify your identity') || lowerText.includes('confirm your identity'))
                                        && (lowerText.includes('send message') || lowerText.includes('send messages'))
                                    )
                                    || (
                                        text.includes('验证')
                                        && text.includes('身份')
                                        && (text.includes('发送消息') || text.includes('傳送訊息'))
                                    )
                                    || (
                                        lowerText.includes('unusual activity')
                                        && (lowerText.includes('restricted') || lowerText.includes('limit'))
                                    )
                                    || (
                                        text.includes('异常活动')
                                        && (text.includes('受到限制') || text.includes('被限制'))
                                    )
                                    || (
                                        text.includes('異常活動')
                                        && (text.includes('受到限制') || text.includes('被限制'))
                                    )
                                    || (
                                        text.includes('ยืนยันข้อมูลระบุตัวตน')
                                        && text.includes('ส่งข้อความ')
                                    )
                                    || (
                                        text.includes('กิจกรรมที่ผิดปกติ')
                                        && text.includes('ถูกจำกัด')
                                    )
                                ) {
                                    return true;
                                }
                                return false;
                            };
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
                            if (matchesIdentityVerificationSendRestriction()) {
                                return {reason: 'send_restricted_ui', alert_text: text};
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
                    const normalize = (value) => String(value || '')
                        .replace(/\\u00a0/g, ' ')
                        .replace(/[\\u200b-\\u200f\\u202a-\\u202e\\u2060-\\u206f\\ufeff]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
                    const expectedText = payload.expectedText;
                    const statusTexts = payload.statusTexts || [];
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
                    let hasSentTime = false;
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
                        if (!hasSentTime && sentTimeRe.test(combined)) {
                            hasSentTime = true;
                        }
                    }

                    const windowText = normalize(root.innerText || '');

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
                        has_sent_time: hasSentTime,
                        window_fingerprint: fingerprintParts.join('|')
                    };
                }""",
                {
                    "expectedText": normalized_text,
                    "statusTexts": self.DELIVERY_STATUS_TEXTS,
                    "messageTexts": self.MEMBER_CARD_MESSAGE_TEXTS,
                    "messageHintKeywords": self.MESSAGE_HEADING_HINT_TOKENS,
                    "ignoredControlTexts": self.CHAT_IGNORED_CONTROL_TEXTS,
                },
            )
        except Exception:
            return None

    def _normalize_text(self, value):
        text = str(value or "")
        if not text:
            return ""
        text = text.replace("\xa0", " ")
        text = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff]", "", text)
        return " ".join(text.split())

    def _matches_identity_verification_send_restricted_text(self, text):
        normalized = self._normalize_text(text)
        if not normalized:
            return False
        lower_text = normalized.lower()
        if self._matches_any_text(normalized, self.SEND_RESTRICTED_VERIFY_IDENTITY_TEXTS):
            return True
        if self._matches_any_text(normalized, self.SEND_RESTRICTED_UNUSUAL_ACTIVITY_TEXTS):
            return True
        if (
            (
                ("verify your identity" in lower_text or "confirm your identity" in lower_text)
                and ("send message" in lower_text or "send messages" in lower_text)
            )
            or (
                "验证" in normalized
                and "身份" in normalized
                and ("发送消息" in normalized or "傳送訊息" in normalized)
            )
            or (
                "unusual activity" in lower_text
                and ("restricted" in lower_text or "limit" in lower_text)
            )
            or (
                "异常活动" in normalized
                and ("受到限制" in normalized or "被限制" in normalized)
            )
            or (
                "異常活動" in normalized
                and ("受到限制" in normalized or "被限制" in normalized)
            )
            or (
                "ยืนยันข้อมูลระบุตัวตน" in normalized
                and "ส่งข้อความ" in normalized
            )
            or (
                "กิจกรรมที่ผิดปกติ" in normalized
                and "ถูกจำกัด" in normalized
            )
        ):
            return True
        return False

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
        if self._matches_any_text(normalized, self.RUNTIME_PERMANENT_SKIP_TEXTS):
            return "configured_permanent_skip"
        if self._matches_any_text(normalized, self.RUNTIME_POPUP_STOP_TEXTS):
            return "send_restricted_ui"
        if self._matches_identity_verification_send_restricted_text(normalized):
            return "send_restricted_ui"
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
            "configured_permanent_skip": "永久跳过提醒词",
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
            "configured_permanent_skip",
        }

    def _default_skip_alert_text(self, reason):
        normalized_reason = str(reason or "").strip()
        if normalized_reason == "messenger_login_required":
            return "对方当前未登录 Messenger，暂时无法发起对话，已跳过。"
        if normalized_reason == "account_cannot_message":
            return "当前无法向该成员发起私聊，已跳过。"
        if normalized_reason == "configured_permanent_skip":
            return "命中永久跳过提醒词，已跳过当前成员。"
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
            if self._matches_identity_verification_send_restricted_text(detail):
                return "当前窗口需要先完成身份验证后才能继续发送消息，且账号因异常活动受到限制，已停止当前窗口，请先完成验证或更换账户。"
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
                    const normalize = (value) => String(value || '')
                        .replace(/\\u00a0/g, ' ')
                        .replace(/[\\u200b-\\u200f\\u202a-\\u202e\\u2060-\\u206f\\ufeff]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
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

    def _dismiss_interfering_popups(self, context="", timeout_ms=None):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0
        try:
            timeout_ms = int(timeout_ms or 0)
        except Exception:
            timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        deadline = time.time() + timeout_ms / 1000 if timeout_ms > 0 else None

        closed_total = 0
        for _ in range(3):
            if deadline is not None and time.time() >= deadline:
                break
            closed_round = 0
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
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
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
            try:
                self.page.mouse.move(2, 2)
                self.page.mouse.click(2, 2)
            except Exception:
                pass
            closed_total += closed_round
            if closed_round <= 0:
                break
            if deadline is not None:
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    break
                self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)
            else:
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
            "message_button_click_failed": f"{self._prefix()}当前成员资料卡中的发消息按钮点击失败，当前轮次跳过: {normalized_name}",
        }
        return self._build_restriction_payload(
            "skip",
            normalized_reason,
            log_text=reason_log_map.get(
                normalized_reason,
                f"{self._prefix()}资料卡按钮阶段命中 {normalized_reason}，当前轮次跳过: {normalized_name}",
            ),
        )

    def _classify_message_button_failure(
        self,
        name,
        hover_card=None,
        timed_out=False,
        has_action_candidates=False,
        has_message_like_candidates=False,
    ):
        if hover_card is None:
            return self._build_message_button_skip_result("member_hover_timeout", name=name)

        if has_action_candidates and not has_message_like_candidates:
            return self._build_message_button_skip_result("message_button_not_found", name=name)

        if timed_out:
            return self._build_message_button_skip_result("message_button_timeout", name=name)

        return self._build_message_button_skip_result("message_button_not_found", name=name)

    def _classify_message_button_click_failure(self, name, restriction=None):
        if restriction:
            return restriction
        return self._build_message_button_skip_result("message_button_click_failed", name=name)

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
            payload.get("delivery_restriction_state"),
            bool(payload.get("shell_seen")),
            bool(payload.get("popup_blocking")),
        )

    def _build_chat_open_result(
        self,
        chat_session=None,
        restriction=None,
        shell_seen=False,
        popup_blocking=False,
    ):
        if chat_session:
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                chat_session = None
        return {
            "chat_session": chat_session,
            "restriction": restriction,
            "delivery_restriction_state": self._map_postsend_restriction_to_delivery_state(
                restriction
            ),
            "shell_seen": bool(shell_seen),
            "popup_blocking": bool(popup_blocking),
        }

    def _build_ready_chat_session_result(
        self,
        chat_session=None,
        restriction=None,
        shell_seen=False,
        ready=False,
        recovered=False,
    ):
        ready = bool(ready and chat_session)
        recovered = bool(recovered and ready and chat_session)
        return {
            "chat_session": chat_session,
            "restriction": restriction,
            "delivery_restriction_state": self._map_postsend_restriction_to_delivery_state(
                restriction
            ),
            "shell_seen": bool(shell_seen),
            "ready": ready,
            "recovered": recovered,
        }

    def _extract_ready_chat_session_result(self, ready_result):
        payload = ready_result if isinstance(ready_result, dict) else {}
        return (
            payload.get("chat_session"),
            payload.get("restriction"),
            payload.get("delivery_restriction_state"),
            bool(payload.get("shell_seen")),
            bool(payload.get("ready")),
            bool(payload.get("recovered")),
        )

    def _build_prechat_send_session_result(self, chat_session=None, dm_result=None):
        if chat_session:
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                chat_session = None
        if dm_result:
            chat_session = None
        return {
            "chat_session": chat_session,
            "dm_result": dm_result,
        }

    def _extract_prechat_send_session_result(self, prechat_result):
        payload = prechat_result if isinstance(prechat_result, dict) else {}
        return (
            payload.get("chat_session"),
            payload.get("dm_result"),
        )

    def _build_postsend_chat_session_result(self, chat_session=None, state=None):
        if chat_session:
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                chat_session = None
        return {
            "chat_session": chat_session,
            "state": state,
        }

    def _extract_postsend_chat_session_result(self, session_result):
        payload = session_result if isinstance(session_result, dict) else {}
        return (
            payload.get("chat_session"),
            payload.get("state"),
        )

    def _build_postsend_ready_session_result(self, state=None, chat_session=None, recovered=False):
        if chat_session:
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                chat_session = None
        if state not in {"ready", "sent", "timeout"}:
            chat_session = None
        recovered = bool(recovered and state == "ready" and chat_session)
        return {
            "state": state,
            "chat_session": chat_session,
            "recovered": recovered,
        }

    def _extract_postsend_ready_session_result(self, ready_result):
        payload = ready_result if isinstance(ready_result, dict) else {}
        return (
            payload.get("state"),
            payload.get("chat_session"),
            bool(payload.get("recovered")),
        )

    def _build_paste_chat_session_result(self, state=None, chat_session=None):
        if state == "ready":
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                state = "chat_editor_not_ready"
        return {
            "state": state,
            "chat_session": chat_session,
        }

    def _extract_paste_chat_session_result(self, paste_result):
        payload = paste_result if isinstance(paste_result, dict) else {}
        return (
            payload.get("state"),
            payload.get("chat_session"),
        )

    def _build_delivery_session_result(self, state=None, chat_session=None):
        if chat_session:
            editor, _ = self._extract_chat_session(chat_session)
            if not editor:
                chat_session = None
        return {
            "state": state,
            "chat_session": chat_session,
        }

    def _extract_delivery_session_result(self, delivery_result):
        payload = delivery_result if isinstance(delivery_result, dict) else {}
        return (
            payload.get("state"),
            payload.get("chat_session"),
        )

    def _build_probe_precheck_result(self, state=None, chat_session=None):
        if state == "sent":
            state = "confirmed_sent"
        return self._build_delivery_session_result(
            state=state,
            chat_session=chat_session,
        )

    def _build_member_link_result(self, link=None, recovered=False):
        recovered = bool(recovered and link)
        return {
            "link": link,
            "recovered": recovered,
        }

    def _extract_member_link_result(self, member_link_result):
        payload = member_link_result if isinstance(member_link_result, dict) else {}
        return (
            payload.get("link"),
            bool(payload.get("recovered")),
        )

    def _build_member_card_button_result(
        self,
        button=None,
        link=None,
        failure=None,
        hover_card=None,
        has_action_candidates=False,
        has_message_like_candidates=False,
        link_box=None,
    ):
        if button is not None:
            failure = None
        return {
            "button": button,
            "link": link,
            "failure": failure,
            "hover_card": hover_card,
            "has_action_candidates": bool(has_action_candidates),
            "has_message_like_candidates": bool(has_message_like_candidates),
            "link_box": link_box,
        }

    def _extract_member_card_button_result(self, button_result):
        payload = button_result if isinstance(button_result, dict) else {}
        return (
            payload.get("button"),
            payload.get("link"),
            payload.get("failure"),
            payload.get("hover_card"),
            bool(payload.get("has_action_candidates")),
            bool(payload.get("has_message_like_candidates")),
            payload.get("link_box"),
        )

    def _ensure_prechat_chat_open_result(
        self,
        button,
        link=None,
        target=None,
        name="",
        expected_name=None,
        timeout_ms=None,
        action_timeout_ms=None,
    ):
        if timeout_ms is None:
            timeout_ms = self._chat_open_timeout_ms("default")
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            return self._build_chat_open_result()

        if action_timeout_ms is None:
            action_timeout_ms = self._scale_ms(2000, min_ms=1500)
        else:
            try:
                action_timeout_ms = int(action_timeout_ms)
            except Exception:
                action_timeout_ms = 0
        action_timeout_ms = max(action_timeout_ms, 0)

        deadline = time.time() + timeout_ms / 1000
        chat_session = None
        restriction = None
        chat_shell_seen = False
        popup_blocking = False

        def remaining_open_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        def build_open_result():
            return self._build_chat_open_result(
                chat_session=chat_session,
                restriction=restriction,
                shell_seen=chat_shell_seen,
                popup_blocking=popup_blocking,
            )

        def consume_open_result(open_result):
            nonlocal chat_session, restriction, chat_shell_seen, popup_blocking
            (
                next_session,
                next_restriction,
                _,
                next_shell_seen,
                next_popup_blocking,
            ) = self._extract_chat_open_result(open_result)
            chat_session = next_session
            restriction = next_restriction
            chat_shell_seen = chat_shell_seen or next_shell_seen
            popup_blocking = popup_blocking or next_popup_blocking

        initial_open_timeout_ms = min(self._chat_open_timeout_ms("initial"), remaining_open_ms())
        consume_open_result(
            self._wait_for_chat_open_result(
                expected_name=expected_name,
                timeout_ms=initial_open_timeout_ms,
                stage="聊天窗口首次复核",
            )
        )
        if chat_session or restriction:
            return build_open_result()

        popup_cleanup_timeout_ms = min(self._scale_ms(900, min_ms=360), remaining_open_ms())
        closed_popups = self._dismiss_interfering_popups(
            context=f"成员 {name} 聊天窗口未打开",
            timeout_ms=popup_cleanup_timeout_ms,
        )
        if closed_popups > 0:
            log.info(f"{self._prefix()}成员 {name} 聊天窗口未打开：已关闭 {closed_popups} 个干扰弹层/提醒。")
        if closed_popups and not chat_shell_seen:
            consume_open_result(
                self._recover_chat_open_after_surface_cleanup(
                    name=name,
                    expected_name=expected_name,
                    stage="聊天窗口弹层清理后重试复核",
                    timeout_ms=min(action_timeout_ms, remaining_open_ms()),
                    button=button,
                    link=link,
                    target=target,
                )
            )
            if chat_session:
                log.info(f"{self._prefix()}关闭干扰弹层并重试点击后，聊天窗口恢复可用: {name}")
        if chat_session or restriction:
            return build_open_result()

        if chat_shell_seen:
            final_restriction = self._detect_prechat_restriction_result(
                name,
                stage="聊天窗口最终限制复核",
            )
            if final_restriction:
                restriction = final_restriction
            return build_open_result()

        overlay_text = self._detect_blocking_overlay()
        if overlay_text:
            popup_blocking = True
            overlay_restriction = self._build_restriction_result(
                self._match_restriction_reason_from_text(overlay_text),
                overlay_text,
                name=name,
                stage="聊天窗口被限制弹层阻断",
            )
            if overlay_restriction:
                restriction = overlay_restriction
                return build_open_result()

        if overlay_text:
            consume_open_result(
                self._recover_chat_open_after_surface_cleanup(
                    name=name,
                    expected_name=expected_name,
                    stage="聊天窗口阻断清理后复核",
                    timeout_ms=remaining_open_ms(),
                    button=button,
                    link=link,
                    target=target,
                )
            )
            if chat_session:
                log.info(f"{self._prefix()}聊天窗口阻断清理后恢复可用: {name}")
                return build_open_result()
            if restriction:
                return build_open_result()

        final_restriction_stage = "聊天窗口被弹层阻断后最终复核" if popup_blocking else "聊天窗口未打开前最终复核"
        final_restriction = self._detect_prechat_restriction_result(
            name,
            stage=final_restriction_stage,
        )
        if final_restriction:
            restriction = final_restriction
            return build_open_result()

        closed_chats = self._close_visible_chat_windows()
        final_retry_timeout_ms = min(action_timeout_ms, remaining_open_ms())
        if final_retry_timeout_ms > 0:
            consume_open_result(
                self._recover_chat_open_after_surface_cleanup(
                    name=name,
                    expected_name=expected_name,
                    stage="聊天窗口重试复核",
                    timeout_ms=final_retry_timeout_ms,
                    button=button,
                    link=link,
                    target=target,
                )
            )
            if chat_session:
                if closed_chats > 0:
                    log.info(f"{self._prefix()}关闭残留聊天窗后，聊天重试恢复成功: {name}")
                else:
                    log.info(f"{self._prefix()}聊天窗口最终重试后恢复成功: {name}")

        return build_open_result()

    def _recover_chat_open_after_surface_cleanup(
        self,
        name,
        expected_name=None,
        stage="",
        timeout_ms=None,
        button=None,
        link=None,
        target=None,
    ):
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return self._build_chat_open_result()
            deadline = time.time() + timeout_ms / 1000

        def remaining_recovery_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        cleanup_timeout_ms = self._scale_ms(1200, min_ms=480)
        if deadline is not None:
            cleanup_timeout_ms = min(cleanup_timeout_ms, remaining_recovery_ms())
            if cleanup_timeout_ms <= 0:
                return self._build_chat_open_result()
        self._run_surface_cleanup(
            context=f"成员 {name} 聊天窗口阻断清理",
            close_chat=False,
            dismiss_popup=True,
            dismiss_card=True,
            timeout_ms=cleanup_timeout_ms,
        )
        requires_retry_click = button is not None or target is not None
        if requires_retry_click:
            retry_click_result = self._click_member_card_message_button(
                button,
                link=link,
                target=target,
                name=name,
                timeout_ms=remaining_recovery_ms(),
            )
            retry_button, _, retry_failure, _, _, _, _ = self._extract_member_card_button_result(
                retry_click_result
            )
            if retry_failure:
                return self._build_chat_open_result(restriction=retry_failure)
            if retry_button is None:
                return self._build_chat_open_result()
        open_timeout_ms = self._chat_open_timeout_ms("retry")
        if deadline is not None:
            open_timeout_ms = min(open_timeout_ms, remaining_recovery_ms())
            if open_timeout_ms <= 0:
                return self._build_chat_open_result()
        open_result = self._wait_for_chat_open_result(
            expected_name=expected_name,
            timeout_ms=open_timeout_ms,
            stage=stage or "聊天窗口阻断清理后复核",
        )
        return open_result

    def _recover_ready_chat_session_after_surface_cleanup(
        self,
        name,
        expected_name=None,
        stage="",
        timeout_ms=None,
        open_timeout_ms=None,
        ready_timeout_ms=None,
    ):
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return self._build_ready_chat_session_result()
            deadline = time.time() + timeout_ms / 1000

        def remaining_recovery_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        cleanup_timeout_ms = self._scale_ms(1200, min_ms=480)
        if deadline is not None:
            cleanup_timeout_ms = min(cleanup_timeout_ms, remaining_recovery_ms())
            if cleanup_timeout_ms <= 0:
                return self._build_ready_chat_session_result()
        self._run_surface_cleanup(
            context=f"成员 {name} 聊天输入框未就绪清理",
            close_chat=False,
            dismiss_popup=True,
            dismiss_card=True,
            timeout_ms=cleanup_timeout_ms,
        )
        if open_timeout_ms is None:
            open_timeout_ms = self._chat_open_timeout_ms("retry")
        if deadline is not None:
            open_timeout_ms = min(open_timeout_ms, remaining_recovery_ms())
            if open_timeout_ms <= 0:
                return self._build_ready_chat_session_result()
        open_result = self._wait_for_chat_open_result(
            expected_name=expected_name,
            timeout_ms=open_timeout_ms,
            stage=stage or "聊天输入框恢复复核",
        )
        chat_session, restriction, _, shell_seen, _ = self._extract_chat_open_result(open_result)
        if restriction or not chat_session:
            return self._build_ready_chat_session_result(
                chat_session=chat_session,
                restriction=restriction,
                shell_seen=shell_seen,
            )

        if ready_timeout_ms is None:
            ready_timeout_ms = self._editor_ready_timeout_ms("default")
        if deadline is not None:
            ready_timeout_ms = min(ready_timeout_ms, remaining_recovery_ms())
            if ready_timeout_ms <= 0:
                return self._build_ready_chat_session_result(
                    chat_session=chat_session,
                    shell_seen=shell_seen,
                )

        def resolved_recovery_session(candidate_session=None):
            session = candidate_session
            if not session:
                return None
            session_editor, _ = self._extract_chat_session(session)
            if not session_editor:
                return None
            visible_timeout_ms = self._scale_ms(180, min_ms=80)
            if deadline is not None:
                visible_timeout_ms = min(visible_timeout_ms, remaining_recovery_ms())
            if visible_timeout_ms <= 0:
                return None
            if self._chat_editor_gone(session_editor, timeout_ms=visible_timeout_ms):
                return None
            return session

        ready_state = self._wait_for_ready_chat_session(
            chat_session,
            expected_name=expected_name,
            timeout_ms=ready_timeout_ms,
            stage=stage or "聊天输入框恢复复核",
        )
        (
            recovered_ready_session,
            ready_restriction,
            _,
            ready_shell_seen,
            ready_available,
            _,
        ) = self._extract_ready_chat_session_result(ready_state)
        return self._build_ready_chat_session_result(
            chat_session=resolved_recovery_session(recovered_ready_session)
            or resolved_recovery_session(chat_session),
            restriction=ready_restriction,
            shell_seen=shell_seen or ready_shell_seen,
            ready=ready_available,
        )

    def _ensure_ready_chat_session(
        self,
        name,
        chat_session,
        expected_name=None,
        stage="",
        timeout_ms=None,
        ready_phase="default",
    ):
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return self._build_ready_chat_session_result(
                    chat_session=chat_session,
                )
            deadline = time.time() + timeout_ms / 1000

        def remaining_ready_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        def visible_ready_session(candidate_session=None):
            if not candidate_session:
                return None
            original_editor, _ = self._extract_chat_session(candidate_session)
            if not original_editor:
                return None
            visible_timeout_ms = self._scale_ms(180, min_ms=80)
            remaining_ms = remaining_ready_ms()
            if remaining_ms is not None:
                visible_timeout_ms = min(visible_timeout_ms, remaining_ms)
            if visible_timeout_ms <= 0:
                return None
            if self._chat_editor_gone(original_editor, timeout_ms=visible_timeout_ms):
                return None
            return candidate_session

        def resolved_ready_session(next_session=None):
            return visible_ready_session(next_session) or visible_ready_session(chat_session)

        ready_timeout_ms = self._editor_ready_timeout_ms(ready_phase)
        if deadline is not None:
            ready_timeout_ms = min(ready_timeout_ms, remaining_ready_ms())
        if ready_timeout_ms and ready_timeout_ms > 0:
            ready_state = self._wait_for_ready_chat_session(
                chat_session,
                expected_name=expected_name,
                timeout_ms=ready_timeout_ms,
                stage=stage or "聊天输入框等待中",
            )
            (
                ready_session,
                ready_restriction,
                _,
                ready_shell_seen,
                ready_available,
                _,
            ) = self._extract_ready_chat_session_result(ready_state)
            if ready_restriction:
                return self._build_ready_chat_session_result(
                    chat_session=resolved_ready_session(ready_session),
                    restriction=ready_restriction,
                    shell_seen=ready_shell_seen,
                )
            if ready_available:
                return self._build_ready_chat_session_result(
                    chat_session=resolved_ready_session(ready_session),
                    shell_seen=ready_shell_seen,
                    ready=True,
                )

        recovery_timeout_ms = remaining_ready_ms()
        if deadline is not None and recovery_timeout_ms <= 0:
            return self._build_ready_chat_session_result(
                chat_session=resolved_ready_session(),
            )

        recovery_ready_timeout_ms = self._editor_ready_timeout_ms(ready_phase)
        if deadline is not None:
            recovery_ready_timeout_ms = min(recovery_ready_timeout_ms, recovery_timeout_ms)

        ready_recovery = self._recover_ready_chat_session_after_surface_cleanup(
            name=name,
            expected_name=expected_name,
            stage=stage,
            timeout_ms=recovery_timeout_ms,
            ready_timeout_ms=recovery_ready_timeout_ms,
        )
        (
            recovered_session,
            recovery_restriction,
            _,
            recovery_shell_seen,
            recovery_ready,
            _,
        ) = self._extract_ready_chat_session_result(ready_recovery)
        if recovery_restriction:
            return self._build_ready_chat_session_result(
                chat_session=resolved_ready_session(recovered_session),
                restriction=recovery_restriction,
                shell_seen=recovery_shell_seen,
            )
        if recovery_ready:
            return self._build_ready_chat_session_result(
                chat_session=resolved_ready_session(recovered_session),
                shell_seen=recovery_shell_seen,
                ready=True,
                recovered=True,
            )
        return self._build_ready_chat_session_result(
            chat_session=resolved_ready_session(recovered_session),
            shell_seen=recovery_shell_seen,
        )

    def _restriction_to_dm_result_with_cleanup(self, restriction, name="", cleanup_context=""):
        if not restriction:
            return None
        normalized_name = str(name or "").strip()
        self._log_restriction_result(restriction)
        self._dismiss_interfering_popups(
            context=cleanup_context or (
                f"成员 {normalized_name} 限制对话框收尾"
                if normalized_name
                else "限制对话框收尾"
            )
        )
        self._close_visible_chat_windows()
        self._dismiss_member_hover_card()
        if normalized_name:
            self._log_limit_dialog_page_state(normalized_name)
        return self._restriction_to_dm_result(restriction)

    def _prepare_prechat_send_session(
        self,
        button,
        link=None,
        target=None,
        name="",
        expected_name=None,
        open_timeout_ms=None,
        action_timeout_ms=None,
        ready_phase="default",
    ):
        open_result = self._ensure_prechat_chat_open_result(
            button=button,
            link=link,
            target=target,
            name=name,
            expected_name=expected_name,
            timeout_ms=open_timeout_ms,
            action_timeout_ms=action_timeout_ms,
        )
        (
            chat_session,
            open_restriction,
            _,
            chat_shell_seen,
            popup_blocking,
        ) = self._extract_chat_open_result(open_result)
        if open_restriction:
            return self._build_prechat_send_session_result(
                dm_result=self._restriction_to_dm_result_with_cleanup(
                    open_restriction,
                    name=name,
                    cleanup_context=f"成员 {name} 限制对话框收尾",
                )
            )
        if not chat_session:
            return self._build_prechat_send_session_result(
                dm_result=self._build_prechat_failure_dm_result(
                    name=name,
                    chat_session=chat_session,
                    shell_seen=chat_shell_seen,
                    popup_blocking=popup_blocking,
                )
            )

        ready_state = self._ensure_ready_chat_session(
            name=name,
            chat_session=chat_session,
            expected_name=expected_name,
            stage="聊天窗口发送前复核",
            timeout_ms=action_timeout_ms,
            ready_phase=ready_phase,
        )
        (
            ready_session,
            ready_restriction,
            _,
            ready_shell_seen,
            ready_available,
            cleanup_recovered,
        ) = self._extract_ready_chat_session_result(ready_state)
        chat_session = ready_session or chat_session
        if ready_restriction:
            return self._build_prechat_send_session_result(
                dm_result=self._restriction_to_dm_result_with_cleanup(
                    ready_restriction,
                    name=name,
                    cleanup_context=f"成员 {name} 限制对话框收尾",
                )
            )
        if not ready_available:
            return self._build_prechat_send_session_result(
                dm_result=self._build_prechat_failure_dm_result(
                    name=name,
                    chat_session=chat_session,
                    shell_seen=chat_shell_seen or ready_shell_seen,
                    popup_blocking=popup_blocking,
                )
            )
        if cleanup_recovered:
            log.info(f"{self._prefix()}聊天输入框恢复后重新就绪，继续执行发送: {name}")

        prechat_restriction = self._detect_prechat_restriction_result(
            name=name,
            stage="聊天窗口发送前复核",
        )
        if prechat_restriction:
            return self._build_prechat_send_session_result(
                dm_result=self._restriction_to_dm_result_with_cleanup(
                    prechat_restriction,
                    name=name,
                    cleanup_context=f"成员 {name} 限制对话框收尾",
                )
            )

        return self._build_prechat_send_session_result(
            chat_session=chat_session,
        )

    def _build_prechat_failure_dm_result(
        self,
        name,
        chat_session=None,
        shell_seen=False,
        popup_blocking=False,
    ):
        if chat_session:
            log.warning(f"{self._prefix()}输入框未准备就绪，当前保留成员待下次重试: {name}")
            return self._build_dm_result("error", "chat_editor_not_ready")

        if popup_blocking:
            log.warning(f"{self._prefix()}聊天窗口被干扰弹层阻断，当前保留成员待下次重试: {name}")
            return self._build_dm_result("error", "chat_popup_blocking")

        if shell_seen:
            alert_text = "右侧聊天窗口已出现，但未识别到稳定可写的输入框或限制提示，请检查当前窗口聊天状态。"
            log.warning(
                f"{self._prefix()}右侧聊天壳已出现，但未识别到稳定可写的输入框或限制提示，停止当前窗口: {name}"
            )
            return self._build_dm_result("blocked", "chat_shell_unrecognized", alert_text)

        log.warning(f"{self._prefix()}聊天窗口在输入框恢复阶段仍未恢复可用，当前保留成员待下次重试: {name}")
        return self._build_dm_result("error", "chat_editor_not_found")

    def _build_chat_session(self, editor, close_button=None):
        if not editor:
            return None
        return {
            "editor": editor,
            "close_button": close_button,
        }

    def _extract_chat_session(self, session):
        payload = session or {}
        return payload.get("editor"), payload.get("close_button")

    def _refresh_chat_session(self, chat_session=None, expected_name=None, timeout_ms=None, allow_generic=True):
        editor, close_button = self._extract_chat_session(chat_session)

        def current_session_if_alive(check_timeout_ms=None):
            if not editor:
                return None
            if check_timeout_ms is None:
                check_timeout_ms = self._scale_ms(180, min_ms=80)
            else:
                try:
                    check_timeout_ms = int(check_timeout_ms)
                except Exception:
                    check_timeout_ms = 0
                check_timeout_ms = max(check_timeout_ms, 1)
            if self._chat_editor_gone(editor, timeout_ms=check_timeout_ms):
                return None
            return self._build_chat_session(editor, close_button=close_button)

        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return current_session_if_alive(1)
            deadline = time.time() + timeout_ms / 1000

        def remaining_refresh_ms():
            if deadline is None:
                return None
            return max(int((deadline - time.time()) * 1000), 0)

        refreshed_session = None
        if expected_name:
            refreshed_timeout_ms = remaining_refresh_ms()
            if refreshed_timeout_ms is None or refreshed_timeout_ms > 0:
                refreshed_session = self._locate_chat_session(
                    expected_name=expected_name,
                    timeout_ms=refreshed_timeout_ms,
                    require_expected_name=not allow_generic,
                )
        if not refreshed_session and allow_generic:
            refreshed_timeout_ms = remaining_refresh_ms()
            if refreshed_timeout_ms is None or refreshed_timeout_ms > 0:
                refreshed_session = self._locate_chat_session(timeout_ms=refreshed_timeout_ms)
        if not refreshed_session:
            return current_session_if_alive(remaining_refresh_ms())

        return refreshed_session

    def _run_surface_cleanup(
        self,
        context="",
        close_chat=True,
        dismiss_popup=True,
        dismiss_card=True,
        timeout_ms=None,
    ):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(1200, min_ms=480)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        deadline = time.time() + timeout_ms / 1000
        closed_chat = 0
        closed_popup = 0
        closed_card = 0
        overlay_blocking = False
        residual_chat = False

        while True:
            cleanup_state = self._inspect_surface_cleanup_state()
            overlay_blocking = bool(self._detect_blocking_overlay()) if dismiss_popup else False
            residual_chat = not self._chat_surfaces_cleared() if close_chat else False
            has_member_card = bool(cleanup_state.get("has_member_card"))

            need_close_chat = close_chat and residual_chat
            need_dismiss_popup = dismiss_popup and (
                cleanup_state.get("has_popup_closer")
                or overlay_blocking
            )
            need_dismiss_card = dismiss_card and has_member_card

            if not need_close_chat and not need_dismiss_popup and not need_dismiss_card:
                break

            if need_close_chat:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                closed_chat += self._close_visible_chat_windows(
                    settled_timeout_ms=remaining_ms,
                    idle_timeout_ms=remaining_ms,
                )
            if need_dismiss_popup:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                closed_popup += self._dismiss_interfering_popups(
                    context=context,
                    timeout_ms=remaining_ms,
                )
            if need_dismiss_card:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                closed_card += self._dismiss_member_hover_card(timeout_ms=remaining_ms)

            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)

        final_state = self._inspect_surface_cleanup_state()
        if dismiss_popup:
            overlay_blocking = bool(self._detect_blocking_overlay())
        if close_chat:
            residual_chat = not self._chat_surfaces_cleared()
        return {
            "closed_chat": closed_chat,
            "closed_popup": closed_popup,
            "closed_card": closed_card,
            "overlay_blocking": overlay_blocking,
            "residual_chat": residual_chat,
            "has_member_card": bool(final_state.get("has_member_card")),
        }

    def _resolve_close_chat_session(self, chat_session, expected_name=None, timeout_ms=None):
        editor, close_button = self._extract_chat_session(chat_session)
        try:
            timeout_ms = int(timeout_ms or 0)
        except Exception:
            timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        deadline = time.time() + timeout_ms / 1000 if timeout_ms > 0 else None
        current_session = self._build_chat_session(editor, close_button=close_button)

        while True:
            refreshed_timeout_ms = (
                max(int((deadline - time.time()) * 1000), 0)
                if deadline is not None
                else None
            )
            refreshed_session = self._refresh_chat_session(
                current_session,
                expected_name=expected_name,
                timeout_ms=refreshed_timeout_ms,
                allow_generic=False,
            )
            if refreshed_session:
                return refreshed_session

            if deadline is None or time.time() >= deadline:
                break
            remaining_seconds = max(deadline - time.time(), 0.0) if deadline is not None else 0.0
            if deadline is not None and remaining_seconds <= 0:
                break
            if deadline is not None:
                self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)
            else:
                self._sleep(0.08, min_seconds=0.02)

        return None

    def _close_visible_chat_windows(self, settled_timeout_ms=None, idle_timeout_ms=None):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0
        if settled_timeout_ms is None:
            settled_timeout_ms = self._scale_ms(900, min_ms=360)
        else:
            try:
                settled_timeout_ms = int(settled_timeout_ms)
            except Exception:
                settled_timeout_ms = 0
            settled_timeout_ms = max(settled_timeout_ms, 0)
        if idle_timeout_ms is None:
            idle_timeout_ms = self._scale_ms(420, min_ms=180)
        else:
            try:
                idle_timeout_ms = int(idle_timeout_ms)
            except Exception:
                idle_timeout_ms = 0
            idle_timeout_ms = max(idle_timeout_ms, 0)
        if settled_timeout_ms <= 0 and idle_timeout_ms <= 0:
            return 0
        total_timeout_ms = max(settled_timeout_ms, idle_timeout_ms)
        idle_step_ms = max(int(total_timeout_ms / 6), self._scale_ms(120, min_ms=60))
        deadline = time.time() + total_timeout_ms / 1000
        closed_total = 0
        while time.time() < deadline:
            if self._chat_surfaces_cleared():
                break
            closed = False
            for selector in self.CHAT_CLOSE_SELECTORS:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                locator = self.page.locator(selector)
                count = locator.count()
                if not count:
                    continue
                for idx in range(count - 1, -1, -1):
                    remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                    if remaining_ms <= 0:
                        break
                    button = locator.nth(idx)
                    try:
                        if not button.is_visible(timeout=min(remaining_ms, self._visible_timeout())):
                            continue
                        self._run_pause_aware_action(
                            lambda step_timeout: button.click(timeout=step_timeout),
                            timeout_ms=min(remaining_ms, self._scale_ms(1500, min_ms=300)),
                        )
                        remaining_seconds = max(deadline - time.time(), 0.0)
                        if remaining_seconds > 0:
                            self._sleep(min(0.08, remaining_seconds), min_seconds=0.0)
                        closed = True
                        closed_total += 1
                    except Exception:
                        continue
            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            if closed:
                if self._wait_for_chat_surfaces_cleared(
                    timeout_ms=min(remaining_ms, settled_timeout_ms)
                ):
                    break
                continue
            if self._wait_for_chat_surfaces_cleared(
                timeout_ms=min(remaining_ms, idle_step_ms, idle_timeout_ms)
            ):
                break
        if closed_total > 0:
            log.info(f"{self._prefix()}已关闭 {closed_total} 个残留聊天窗口。")
        return closed_total

    def _dismiss_member_hover_card(self, timeout_ms=None):
        try:
            if self.page.is_closed():
                return 0
        except Exception:
            return 0
        try:
            timeout_ms = int(timeout_ms or 0)
        except Exception:
            timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        deadline = time.time() + timeout_ms / 1000 if timeout_ms > 0 else None
        # 多轮关闭，避免浮层叠加
        closed_total = 0
        for _ in range(5):
            if deadline is not None and time.time() >= deadline:
                break
            closed_any = False
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
            try:
                self.page.keyboard.press("Escape")
            except Exception:
                pass
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
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
                        remaining_ms = max(
                            int((deadline - time.time()) * 1000),
                            0,
                        ) if deadline is not None else 0
                        if deadline is not None and remaining_ms <= 0:
                            break
                        btn = locator.nth(idx)
                        visible_timeout_ms = (
                            min(remaining_ms, self._scale_ms(220, min_ms=160))
                            if deadline is not None
                            else self._scale_ms(220, min_ms=160)
                        )
                        if visible_timeout_ms <= 0:
                            continue
                        if not btn.is_visible(timeout=visible_timeout_ms):
                            continue
                        try:
                            click_timeout_ms = (
                                min(remaining_ms, self._scale_ms(450, min_ms=180))
                                if deadline is not None
                                else self._scale_ms(450, min_ms=180)
                            )
                            if click_timeout_ms <= 0:
                                continue
                            self._run_pause_aware_action(
                                lambda step_timeout: btn.click(timeout=step_timeout),
                                timeout_ms=click_timeout_ms,
                            )
                            closed_any = True
                            closed_total += 1
                        except Exception:
                            continue
                except Exception:
                    continue
            remaining_ms = max(int((deadline - time.time()) * 1000), 0) if deadline is not None else 0
            if deadline is not None and remaining_ms <= 0:
                break
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
            if deadline is not None:
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    break
                self._sleep(min(0.04, remaining_seconds), min_seconds=0.0)
            else:
                self._sleep(0.04)
            if not closed_any:
                break
        if closed_total > 0:
            log.info(f"{self._prefix()}已关闭 {closed_total} 个成员资料卡/悬浮层。")
        return closed_total

    def _ensure_member_link(self, target, name="", timeout_ms=None):
        if timeout_ms is None:
            total_timeout_ms = self._scale_ms(4000, min_ms=900)
        else:
            try:
                total_timeout_ms = int(timeout_ms)
            except Exception:
                total_timeout_ms = 0
            total_timeout_ms = max(total_timeout_ms, 0)
        if total_timeout_ms <= 0:
            return self._build_member_link_result()
        deadline = time.time() + total_timeout_ms / 1000

        def remaining_lookup_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        link = self._find_member_link(target, timeout_ms=remaining_lookup_ms())
        if link:
            return self._build_member_link_result(link=link)

        cleanup_timeout_ms = min(self._scale_ms(1200, min_ms=480), remaining_lookup_ms())
        if cleanup_timeout_ms > 0:
            self._run_surface_cleanup(
                context=f"成员 {name} 成员链接定位恢复",
                close_chat=False,
                dismiss_popup=True,
                dismiss_card=True,
                timeout_ms=cleanup_timeout_ms,
            )

        recovered_link = None
        if remaining_lookup_ms() > 0:
            recovered_link = self._find_member_link(target, timeout_ms=remaining_lookup_ms())
        return self._build_member_link_result(
            link=recovered_link,
            recovered=bool(recovered_link),
        )

    def _find_member_link(self, target, timeout_ms=None):
        user_id = target.get("user_id")
        group_user_url = target.get("group_user_url")
        target_name = str(target.get("name") or "").strip()
        link_selector = str(target.get("link_selector") or "").strip()
        abs_top = target.get("abs_top")
        abs_left = target.get("abs_left")

        if timeout_ms is None:
            total_timeout_ms = self._scale_ms(4000, min_ms=900)
        else:
            try:
                total_timeout_ms = int(timeout_ms)
            except Exception:
                total_timeout_ms = 0
            total_timeout_ms = max(total_timeout_ms, 0)
        if total_timeout_ms <= 0:
            return None
        deadline = time.time() + total_timeout_ms / 1000

        def remaining_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        if link_selector:
            direct_locator = self._find_member_link_by_selector(
                link_selector,
                timeout_ms=remaining_ms(),
            )
            if direct_locator:
                return direct_locator

        selectors = []
        if group_user_url:
            selectors.append(f'a[href="{group_user_url}"]')
        if user_id:
            selectors.append(f'a[href*="/groups/"][href*="/user/{user_id}/"]')
            selectors.append(f'a[href*="/user/{user_id}/"]')

        def _collect_candidates():
            current_remaining_ms = remaining_ms()
            if current_remaining_ms <= 0:
                return []
            candidates = []
            for selector in selectors:
                locator = self.page.locator(selector)
                count = locator.count()
                for idx in range(count):
                    current_remaining_ms = remaining_ms()
                    if current_remaining_ms <= 0:
                        break
                    link = locator.nth(idx)
                    try:
                        visible_timeout_ms = min(self._scale_ms(700, min_ms=260), current_remaining_ms)
                        if visible_timeout_ms <= 0:
                            break
                        if not link.is_visible(timeout=visible_timeout_ms):
                            continue
                        box = self._safe_bounding_box(
                            link,
                            timeout_ms=min(900, current_remaining_ms),
                        )
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
                    timeout_ms=current_remaining_ms,
                )
                if fallback:
                    candidates.append(fallback)
            return candidates

        wait_timeout = min(self._scale_ms(1200, min_ms=700), remaining_ms())
        candidates = self._wait_for_condition(
            "成员链接",
            "等待成员列表中的目标链接稳定可见",
            lambda: _collect_candidates() or None,
            timeout_ms=wait_timeout,
            interval=0.12,
        )
        if not candidates and abs_top is not None and remaining_ms() > 0:
            try:
                scroll_target = max(0, int(abs_top) - 220)
                self.page.evaluate("(y) => window.scrollTo(0, y)", scroll_target)
                settle_timeout_ms = min(self._scale_ms(1400, min_ms=900), remaining_ms())
                self._wait_for_condition(
                    "成员链接",
                    "等待滚动后的成员列表稳定",
                    lambda: self._find_visible_member_link_by_target(
                        user_id=user_id,
                        group_user_url=group_user_url,
                        target_name=target_name,
                        abs_top=abs_top,
                        abs_left=abs_left,
                        timeout_ms=remaining_ms(),
                    ),
                    timeout_ms=settle_timeout_ms,
                    interval=0.12,
                )
            except Exception:
                pass
            retry_timeout_ms = min(self._scale_ms(1400, min_ms=900), remaining_ms())
            candidates = self._wait_for_condition(
                "成员链接",
                "等待滚动后的目标链接稳定可见",
                lambda: _collect_candidates() or None,
                timeout_ms=retry_timeout_ms,
                interval=0.12,
            )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _find_member_link_by_selector(self, selector, timeout_ms=None):
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(260, min_ms=100)
            box_timeout_ms = 320
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            visible_timeout_ms = min(self._scale_ms(260, min_ms=100), timeout_ms)
            box_timeout_ms = min(320, timeout_ms)
        try:
            locator = self.page.locator(selector).first
            if not locator.is_visible(timeout=visible_timeout_ms):
                return None
            box = self._safe_bounding_box(locator, timeout_ms=box_timeout_ms)
            if not box:
                return None
            return locator
        except Exception:
            return None

    def _find_visible_member_link_by_target(
        self,
        user_id=None,
        group_user_url=None,
        target_name="",
        abs_top=None,
        abs_left=None,
        timeout_ms=None,
    ):
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(700, min_ms=260)
            box_timeout_ms = 900
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            visible_timeout_ms = min(self._scale_ms(700, min_ms=260), timeout_ms)
            box_timeout_ms = min(900, timeout_ms)
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
            if not locator.is_visible(timeout=visible_timeout_ms):
                return None
            box = self._safe_bounding_box(locator, timeout_ms=box_timeout_ms)
            if not box:
                return None
            dist_y = abs(box["y"] - abs_top) if abs_top is not None else box["y"]
            dist_x = abs(box["x"] - abs_left) if abs_left is not None else box["x"]
            return (dist_y + dist_x, box["y"], box["x"], locator)
        except Exception:
            return None

    def _resolve_member_card_message_button(self, link, name, timeout_ms=None):
        if timeout_ms is None:
            hover_wait_total_ms = self._scale_ms(max(1, int(self.hover_card_wait_timeout_ms or 0)), min_ms=320)
        else:
            try:
                hover_wait_total_ms = int(timeout_ms)
            except Exception:
                hover_wait_total_ms = 0
            hover_wait_total_ms = max(hover_wait_total_ms, 0)
        if hover_wait_total_ms <= 0:
            return self._build_member_card_button_result(
                link=link,
                failure=self._classify_message_button_failure(
                    name,
                    hover_card=None,
                    timed_out=False,
                ),
            )
        deadline = time.time() + hover_wait_total_ms / 1000

        def remaining_hover_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        link_box = None
        first_hover_wait_ms = max(1, int(hover_wait_total_ms * 0.65))
        first_wait_ms = min(first_hover_wait_ms, remaining_hover_ms())
        hover_wait_result = self._wait_for_hover_card_container(
            link,
            link_box,
            timeout_ms=first_wait_ms,
            detail="等待资料卡区域渲染完成",
        )
        _, _, _, hover_card, _, _, waited_link_box = self._extract_member_card_button_result(
            hover_wait_result
        )
        link_box = waited_link_box

        if hover_card is None:
            cleanup_timeout_ms = min(self._scale_ms(1200, min_ms=480), remaining_hover_ms())
            self._run_surface_cleanup(
                context=f"成员 {name} 资料卡识别阶段",
                close_chat=True,
                dismiss_popup=True,
                dismiss_card=True,
                timeout_ms=cleanup_timeout_ms,
            )
            second_wait_ms = remaining_hover_ms()
            hover_wait_result = self._wait_for_hover_card_container(
                link,
                link_box,
                timeout_ms=second_wait_ms,
                detail="二次等待资料卡区域渲染完成",
            )
            _, _, _, hover_card, _, _, waited_link_box = self._extract_member_card_button_result(
                hover_wait_result
            )
            link_box = waited_link_box

        if hover_card is None:
            return self._build_member_card_button_result(
                link=link,
                failure=self._classify_message_button_failure(
                    name,
                    hover_card=None,
                    timed_out=False,
                ),
            )

        button_wait_ms = remaining_hover_ms()
        button_wait_result = self._wait_for_message_button_in_hover_card(
            link,
            link_box,
            hover_card,
            timeout_ms=button_wait_ms,
        )
        (
            button,
            _,
            _,
            hover_card,
            has_action_candidates,
            has_message_like_candidates,
            _,
        ) = self._extract_member_card_button_result(button_wait_result)
        if button:
            return button_wait_result

        return self._build_member_card_button_result(
            link=link,
            failure=self._classify_message_button_failure(
                name,
                hover_card=hover_card,
                timed_out=True,
                has_action_candidates=has_action_candidates,
                has_message_like_candidates=has_message_like_candidates,
            ),
        )

    def _collect_action_candidates_in_container(self, container):
        try:
            return container.evaluate(
                """(node, payload) => {
                    const normalize = (value) => String(value || '')
                        .replace(/\\u00a0/g, ' ')
                        .replace(/[\\u200b-\\u200f\\u202a-\\u202e\\u2060-\\u206f\\ufeff]/g, '')
                        .replace(/\\s+/g, ' ')
                        .trim();
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
                    const nodes = Array.from(node.querySelectorAll(payload.selector));
                    for (let idx = 0; idx < nodes.length; idx += 1) {
                        const el = nodes[idx];
                        if (!isVisible(el)) continue;
                        if ((el.getAttribute && el.getAttribute('aria-disabled')) === 'true') continue;
                        if (el.hasAttribute && el.hasAttribute('disabled')) continue;
                        const label = normalize(el.getAttribute && el.getAttribute('aria-label'));
                        const text = normalize(el.innerText || el.textContent || '');
                        const combined = normalize(`${label} ${text}`);
                        if (!combined) continue;
                        const role = normalize(el.getAttribute && el.getAttribute('role')).toLowerCase();
                        const tag = normalize(el.tagName).toLowerCase();
                        const href = normalize(el.getAttribute && el.getAttribute('href'));
                        const tabIndex = Number.isFinite(el.tabIndex) ? el.tabIndex : -1;
                        const style = window.getComputedStyle(el);
                        const cursor = normalize(style && style.cursor).toLowerCase();
                        const nativeAction = tag === 'button' || (tag === 'a' && href);
                        const semanticAction = role === 'button' || role === 'link';
                        const keyboardAction = tabIndex >= 0;
                        const pointerAction = cursor === 'pointer';
                        const actionable = nativeAction || semanticAction || keyboardAction || pointerAction;
                        if (!actionable) continue;
                        let clickScore = 0;
                        if (nativeAction) clickScore += 8;
                        if (semanticAction) clickScore += 6;
                        if (keyboardAction) clickScore += 3;
                        if (pointerAction) clickScore += 2;
                        if (href) clickScore += 2;
                        results.push({
                            index: idx,
                            combined,
                            tag,
                            role,
                            href,
                            tabIndex,
                            clickScore,
                        });
                    }
                    return results;
                }""",
                {"selector": self.ACTION_ELEMENT_SELECTOR},
            ) or []
        except Exception:
            return []

    def _find_message_button_in_container(self, container, link=None, hover_card=None):
        candidates = self._collect_action_candidates_in_container(container)
        if not candidates:
            return self._build_member_card_button_result(
                link=link,
                hover_card=hover_card,
            )
        candidate_locator = container.locator(self.ACTION_ELEMENT_SELECTOR)
        normalized_exact_tokens = {
            self._normalize_text(token).lower()
            for token in self.MEMBER_CARD_MESSAGE_TEXTS
            if self._normalize_text(token)
        }
        hint_keywords = tuple(
            self._normalize_text(keyword).lower()
            for keyword in self.MEMBER_CARD_MESSAGE_HINT_KEYWORDS
            if self._normalize_text(keyword)
        )
        best_candidate = None
        has_message_like_candidates = False
        for candidate in candidates:
            combined = self._normalize_text(candidate.get("combined") or "")
            candidate_index = candidate.get("index")
            if candidate_index is None:
                continue
            combined_lower = combined.lower()
            exact_match = combined_lower in normalized_exact_tokens
            partial_match = self._matches_any_text(combined, self.MEMBER_CARD_MESSAGE_TEXTS)
            hint_match = any(keyword in combined_lower for keyword in hint_keywords)
            if not (exact_match or partial_match or hint_match):
                continue
            has_message_like_candidates = True
            score = int(candidate.get("clickScore") or 0)
            if exact_match:
                score += 120
            elif partial_match:
                score += 80
            elif hint_match:
                score += 40
            if (candidate.get("tag") or "") == "button":
                score += 10
            if (candidate.get("role") or "") == "button":
                score += 6
            if candidate.get("href"):
                score += 4
            ranking = (score, -len(combined_lower), -int(candidate_index))
            if best_candidate is None or ranking > best_candidate[0]:
                best_candidate = (ranking, int(candidate_index))
        if best_candidate is None:
            return self._build_member_card_button_result(
                link=link,
                hover_card=hover_card,
                has_action_candidates=True,
                has_message_like_candidates=has_message_like_candidates,
            )
        try:
            return self._build_member_card_button_result(
                button=candidate_locator.nth(best_candidate[1]),
                link=link,
                hover_card=hover_card,
                has_action_candidates=True,
                has_message_like_candidates=True,
            )
        except Exception:
            return self._build_member_card_button_result(
                link=link,
                hover_card=hover_card,
                has_action_candidates=True,
                has_message_like_candidates=has_message_like_candidates,
            )

    def _resolve_clickable_member_card_button(self, button, link=None, target=None, name="", timeout_ms=None):
        candidate_button = button
        try:
            visible_timeout_ms = self._scale_ms(220, min_ms=100)
            if timeout_ms is not None:
                try:
                    timeout_ms = int(timeout_ms)
                except Exception:
                    timeout_ms = 0
                timeout_ms = max(timeout_ms, 0)
                if timeout_ms <= 0:
                    return self._build_member_card_button_result(link=link)
                visible_timeout_ms = min(visible_timeout_ms, timeout_ms)
            if candidate_button is not None and candidate_button.is_visible(timeout=visible_timeout_ms):
                return self._build_member_card_button_result(
                    button=candidate_button,
                    link=link,
                )
        except Exception:
            candidate_button = None

        current_link = link
        if current_link is None and target is not None:
            member_link_result = self._ensure_member_link(
                target,
                name=name,
                timeout_ms=timeout_ms,
            )
            current_link, _ = self._extract_member_link_result(member_link_result)
        if current_link is None:
            return self._build_member_card_button_result()

        return self._resolve_member_card_message_button(
            current_link,
            name,
            timeout_ms=timeout_ms,
        )

    def _click_member_card_message_button(self, button, target=None, name="", timeout_ms=None, link=None):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(1500, min_ms=600)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            return self._build_member_card_button_result(link=link)
        deadline = time.time() + timeout_ms / 1000

        def remaining_click_ms():
            return max(int((deadline - time.time()) * 1000), 0)

        current_link = link
        current_button = button

        def resolve_click_target():
            click_resolve_result = self._resolve_clickable_member_card_button(
                current_button,
                link=current_link,
                target=target,
                name=name,
                timeout_ms=remaining_click_ms(),
            )
            return click_resolve_result

        for attempt in range(2):
            if current_button is None:
                click_resolve_result = resolve_click_target()
                current_button, current_link, resolve_failure, _, _, _, _ = (
                    self._extract_member_card_button_result(click_resolve_result)
                )
                if resolve_failure:
                    return click_resolve_result
                if current_button is None:
                    break
            else:
                remaining_ms = remaining_click_ms()
                if remaining_ms <= 0:
                    break
                visible_timeout_ms = min(self._scale_ms(220, min_ms=100), remaining_ms)
                if visible_timeout_ms <= 0:
                    break
                try:
                    if not current_button.is_visible(timeout=visible_timeout_ms):
                        current_button = None
                        continue
                except Exception:
                    current_button = None
                    continue
            remaining_ms = remaining_click_ms()
            if remaining_ms <= 0:
                break
            try:
                self._run_pause_aware_action(
                    lambda step_timeout: current_button.scroll_into_view_if_needed(timeout=step_timeout),
                    timeout_ms=remaining_ms,
                )
            except Exception:
                pass
            self._wait_for_stable_box(
                current_button,
                max_rounds=2,
                delay=0.05,
                timeout_ms=remaining_ms,
            )
            try:
                remaining_ms = remaining_click_ms()
                if remaining_ms <= 0:
                    break
                self._run_pause_aware_action(
                    lambda step_timeout: current_button.click(timeout=step_timeout),
                    timeout_ms=remaining_ms,
                )
                log.info(f"{self._prefix()}发消息按钮点击成功: {name}")
                return self._build_member_card_button_result(
                    button=current_button,
                    link=current_link,
                )
            except Exception:
                try:
                    current_button.evaluate("el => el.click()")
                    log.info(f"{self._prefix()}发消息按钮点击成功(JS): {name}")
                    return self._build_member_card_button_result(
                        button=current_button,
                        link=current_link,
                    )
                except Exception:
                    pass

            if attempt >= 1:
                break
            recovery_timeout_ms = min(self._scale_ms(900, min_ms=360), remaining_click_ms())
            if recovery_timeout_ms > 0:
                self._dismiss_interfering_popups(
                    context=f"成员 {name} 发消息按钮点击恢复",
                    timeout_ms=recovery_timeout_ms,
                )
                if current_link is not None:
                    try:
                        self._run_pause_aware_action(
                            lambda step_timeout: current_link.scroll_into_view_if_needed(timeout=step_timeout),
                            timeout_ms=recovery_timeout_ms,
                        )
                    except Exception:
                        pass
            current_button = None
        click_failure = self._detect_prechat_restriction_result(
            name,
            stage="发消息按钮点击失败后复核",
        )
        return self._build_member_card_button_result(
            link=current_link,
            failure=self._classify_message_button_click_failure(
                name=name,
                restriction=click_failure,
            ),
        )

    def _matches_any_text(self, text, tokens):
        normalized = self._normalize_text(text).lower()
        if not normalized:
            return False
        for token in tokens or []:
            normalized_token = self._normalize_text(token).lower()
            if normalized_token and normalized_token in normalized:
                return True
        return False

    def _find_hover_card_container(self, link_box, timeout_ms=None):
        if not link_box:
            return None
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            deadline = time.time() + timeout_ms / 1000
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
                remaining_ms = None
                if deadline is not None:
                    remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                    if remaining_ms <= 0:
                        break
                card = locator.nth(idx)
                try:
                    visible_timeout_ms = self._scale_ms(260, min_ms=160)
                    if remaining_ms is not None:
                        visible_timeout_ms = min(remaining_ms, visible_timeout_ms)
                        if visible_timeout_ms <= 0:
                            break
                    if not card.is_visible(timeout=visible_timeout_ms):
                        continue
                    if self._container_is_chat_surface(card):
                        continue
                    box_timeout_ms = 400 if remaining_ms is None else min(remaining_ms, 400)
                    box = self._safe_bounding_box(card, timeout_ms=box_timeout_ms)
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
            if deadline is not None and max(int((deadline - time.time()) * 1000), 0) <= 0:
                break
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][3]

    def _wait_for_hover_card_container(self, link, link_box, timeout_ms, detail):
        try:
            timeout_ms = int(timeout_ms)
        except Exception:
            timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            log.warning(f"{self._prefix()}成员资料卡等待超时，已等待 0.0 秒，{detail}失败")
            return self._build_member_card_button_result(
                hover_card=None,
                link_box=link_box,
            )
        start = time.time()
        deadline = start + timeout_ms / 1000
        current_link_box = link_box
        next_hover_retry_at = start
        log.info(f"{self._prefix()}成员资料卡，{detail}...")
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds
                next_hover_retry_at += paused_seconds

            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            hover_card = self._find_hover_card_container(current_link_box, timeout_ms=remaining_ms)
            if hover_card is not None:
                waited = time.time() - start
                log.info(f"{self._prefix()}成员资料卡等待成功，耗时 {waited:.1f} 秒。")
                return self._build_member_card_button_result(
                    hover_card=hover_card,
                    link_box=current_link_box,
                )

            refreshed_box = self._wait_for_stable_box(
                link,
                max_rounds=2,
                delay=0.06,
                timeout_ms=remaining_ms,
            )
            if refreshed_box:
                current_link_box = refreshed_box
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                hover_card = self._find_hover_card_container(current_link_box, timeout_ms=remaining_ms)
                if hover_card is not None:
                    waited = time.time() - start
                    log.info(f"{self._prefix()}成员资料卡等待成功，耗时 {waited:.1f} 秒。")
                    return self._build_member_card_button_result(
                        hover_card=hover_card,
                        link_box=current_link_box,
                    )

            now = time.time()
            if now >= next_hover_retry_at:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                self._trigger_hover_card(link, timeout_ms=remaining_ms)
                next_hover_retry_at = now + 0.45
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)

        waited = time.time() - start
        log.warning(f"{self._prefix()}成员资料卡等待超时，已等待 {waited:.1f} 秒，{detail}失败")
        return self._build_member_card_button_result(
            hover_card=None,
            link_box=current_link_box,
        )

    def _wait_for_message_button_in_hover_card(self, link, link_box, hover_card, timeout_ms):
        try:
            timeout_ms = int(timeout_ms)
        except Exception:
            timeout_ms = 0
        timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            log.warning(f"{self._prefix()}成员资料卡等待超时，已等待 0.0 秒，等待发消息按钮渲染完成失败")
            return self._build_member_card_button_result(
                link=link,
                hover_card=hover_card,
            )
        start = time.time()
        deadline = start + timeout_ms / 1000
        current_link_box = link_box
        current_hover_card = hover_card
        next_hover_retry_at = start + 0.35
        has_action_candidates = False
        has_message_like_candidates = False
        action_only_since = None
        action_only_grace_ms = max(
            1,
            min(
                timeout_ms,
                min(
                    self._scale_ms(1500, min_ms=700),
                    max(700, int(timeout_ms * 0.35)),
                ),
            ),
        )
        log.info(f"{self._prefix()}成员资料卡，等待发消息按钮渲染完成...")
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds
                next_hover_retry_at += paused_seconds
                if action_only_since is not None:
                    action_only_since += paused_seconds

            if current_hover_card is not None:
                button_result = self._find_message_button_in_container(
                    current_hover_card,
                    link=link,
                    hover_card=current_hover_card,
                )
                button, _, _, _, has_action_candidates, has_message_like_candidates, _ = (
                    self._extract_member_card_button_result(button_result)
                )
                if button is not None:
                    waited = time.time() - start
                    log.info(f"{self._prefix()}成员资料卡等待成功，耗时 {waited:.1f} 秒。")
                    return button_result
                if has_action_candidates and not has_message_like_candidates:
                    if action_only_since is None:
                        action_only_since = time.time()
                    elif (time.time() - action_only_since) * 1000 >= action_only_grace_ms:
                        waited = time.time() - start
                        log.info(
                            f"{self._prefix()}成员资料卡已稳定显示非消息类操作，提前结束等待，耗时 {waited:.1f} 秒。"
                        )
                        return button_result
                else:
                    action_only_since = None

            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            refreshed_box = self._wait_for_stable_box(
                link,
                max_rounds=2,
                delay=0.06,
                timeout_ms=remaining_ms,
            )
            if refreshed_box:
                current_link_box = refreshed_box
            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            refreshed_hover_card = self._find_hover_card_container(current_link_box, timeout_ms=remaining_ms)
            if refreshed_hover_card is not None:
                current_hover_card = refreshed_hover_card
                button_result = self._find_message_button_in_container(
                    current_hover_card,
                    link=link,
                    hover_card=current_hover_card,
                )
                button, _, _, _, has_action_candidates, has_message_like_candidates, _ = (
                    self._extract_member_card_button_result(button_result)
                )
                if button is not None:
                    waited = time.time() - start
                    log.info(f"{self._prefix()}成员资料卡等待成功，耗时 {waited:.1f} 秒。")
                    return button_result
                if has_action_candidates and not has_message_like_candidates:
                    if action_only_since is None:
                        action_only_since = time.time()
                    elif (time.time() - action_only_since) * 1000 >= action_only_grace_ms:
                        waited = time.time() - start
                        log.info(
                            f"{self._prefix()}成员资料卡已稳定显示非消息类操作，提前结束等待，耗时 {waited:.1f} 秒。"
                        )
                        return button_result
                else:
                    action_only_since = None

            now = time.time()
            if current_hover_card is None or now >= next_hover_retry_at:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                self._trigger_hover_card(link, timeout_ms=remaining_ms)
                next_hover_retry_at = now + 0.45
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)

        waited = time.time() - start
        log.warning(f"{self._prefix()}成员资料卡等待超时，已等待 {waited:.1f} 秒，等待发消息按钮渲染完成失败")
        return self._build_member_card_button_result(
            link=link,
            hover_card=current_hover_card,
            has_action_candidates=has_action_candidates,
            has_message_like_candidates=has_message_like_candidates,
        )

    def _container_is_chat_surface(self, container):
        try:
            return bool(
                container.evaluate(
                    """(node, payload) => {
                        if (!node) return false;
                        const editorSelectors = payload.editorSelectors || [];
                        const chatCloseSelectors = payload.chatCloseSelectors || [];
                        const isVisible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;
                            return style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0';
                        };
                        const matchesAnySelectorInside = (root, selectors) => {
                            for (const selector of selectors) {
                                try {
                                    if (Array.from(root.querySelectorAll(selector)).some((el) => isVisible(el))) {
                                        return true;
                                    }
                                } catch (e) {}
                            }
                            return false;
                        };
                        return (
                            matchesAnySelectorInside(node, editorSelectors)
                            || matchesAnySelectorInside(node, chatCloseSelectors)
                        );
                    }""",
                    {
                        "editorSelectors": self.MESSAGE_EDITOR_SELECTORS,
                        "chatCloseSelectors": self.CHAT_CLOSE_SELECTORS,
                    },
                )
            )
        except Exception:
            return False

    def _trigger_hover_card(self, link, timeout_ms=None):
        triggered = False
        hover_timeout_ms = self._scale_ms(900, min_ms=450)
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return False
            hover_timeout_ms = min(hover_timeout_ms, timeout_ms)
        try:
            self._run_pause_aware_action(
                lambda step_timeout: link.hover(timeout=step_timeout),
                timeout_ms=hover_timeout_ms,
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

    def _wait_for_stable_box(self, locator, max_rounds=3, tolerance=2.0, delay=0.15, timeout_ms=None):
        deadline = None
        if timeout_ms is not None:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            deadline = time.time() + timeout_ms / 1000
        last = None
        for _ in range(max_rounds):
            step_timeout_ms = 650
            if deadline is not None:
                remaining_ms = max(int((deadline - time.time()) * 1000), 0)
                if remaining_ms <= 0:
                    break
                step_timeout_ms = min(step_timeout_ms, remaining_ms)
            box = self._safe_bounding_box(locator, timeout_ms=step_timeout_ms)
            if not box:
                if deadline is not None:
                    remaining_seconds = max(deadline - time.time(), 0.0)
                    if remaining_seconds <= 0:
                        break
                    self._sleep(min(delay, remaining_seconds), min_seconds=0.0)
                else:
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
            if deadline is not None:
                remaining_seconds = max(deadline - time.time(), 0.0)
                if remaining_seconds <= 0:
                    break
                self._sleep(min(delay, remaining_seconds), min_seconds=0.0)
            else:
                self._sleep(delay, min_seconds=0.02)
        return last

    def _safe_bounding_box(self, locator, timeout_ms=None):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(700, min_ms=250)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
        try:
            return locator.bounding_box(timeout=timeout_ms)
        except Exception:
            return None

    def _wait_for_chat_open_result(self, expected_name=None, timeout_ms=10000, stage=""):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(10000, min_ms=1600)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            return self._build_chat_open_result()
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
            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            require_expected_name = bool(expected_name)
            if require_expected_name and (
                shell_seen
                or deadline - time.time() <= shell_grace_ms / 1000
            ):
                require_expected_name = False
            chat_session = self._locate_chat_session(
                expected_name=expected_name,
                timeout_ms=remaining_ms,
                require_expected_name=require_expected_name,
            )
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
            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            shell = self._locate_chat_shell(
                expected_name=expected_name,
                timeout_ms=remaining_ms,
            )
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
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)
        waited = time.time() - start
        detail = "等待聊天输入框或限制提示出现失败"
        if shell_seen:
            detail = "右侧聊天壳已出现，但未识别到可用输入框或限制提示"
        log.warning(f"{self._prefix()}聊天窗口等待超时，已等待 {waited:.1f} 秒，{detail}")
        return self._build_chat_open_result(shell_seen=shell_seen)

    def _locate_chat_shell(self, expected_name=None, timeout_ms=None):
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(200, min_ms=80)
            box_timeout_ms = 320
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            visible_timeout_ms = min(self._scale_ms(200, min_ms=80), timeout_ms)
            box_timeout_ms = min(320, timeout_ms)
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
                    if not button.is_visible(timeout=visible_timeout_ms):
                        continue
                    box = self._safe_bounding_box(button, timeout_ms=box_timeout_ms)
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

    def _locate_chat_session(self, expected_name=None, timeout_ms=None, require_expected_name=False):
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(200, min_ms=80)
            box_timeout_ms = 320
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            visible_timeout_ms = min(self._scale_ms(200, min_ms=80), timeout_ms)
            box_timeout_ms = min(320, timeout_ms)
        candidates = []
        for selector in self.MESSAGE_EDITOR_SELECTORS:
            locator = self.page.locator(selector)
            count = locator.count()
            for idx in range(count - 1, -1, -1):
                editor = locator.nth(idx)
                try:
                    if not editor.is_visible(timeout=visible_timeout_ms):
                        continue
                    editor_box = self._safe_bounding_box(editor, timeout_ms=box_timeout_ms)
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
                    if expected_name and require_expected_name and not mentions_expected_name:
                        continue

                    active_window = self._editor_window_is_active(editor)
                    close_button = self._find_chat_close_button(
                        editor,
                        timeout_ms=box_timeout_ms,
                    )

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

    def _wait_for_ready_chat_session(self, chat_session, expected_name=None, timeout_ms=None, stage=""):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(self.editor_ready_wait_timeout_ms, min_ms=1500)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            return self._build_ready_chat_session_result(chat_session=chat_session)
        start = time.time()
        deadline = start + timeout_ms / 1000
        current_session = chat_session
        shell_seen = False
        log.info(f"{self._prefix()}聊天输入框，等待输入框可见且可写...")
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds

            editor, close_button = self._extract_chat_session(current_session)
            current_ready_check_ms = max(int((deadline - time.time()) * 1000), 0)
            if editor and self._editor_ready(editor, timeout_ms=current_ready_check_ms):
                waited = time.time() - start
                log.info(f"{self._prefix()}聊天输入框等待成功，耗时 {waited:.1f} 秒。")
                return self._build_ready_chat_session_result(
                    chat_session=self._build_chat_session(editor, close_button=close_button),
                    shell_seen=shell_seen,
                    ready=True,
                )

            restriction = self._detect_chat_restriction(
                expected_name=expected_name,
                stage=stage or "聊天输入框等待中",
            )
            if restriction:
                waited = time.time() - start
                log.info(f"{self._prefix()}聊天输入框限制识别完成，耗时 {waited:.1f} 秒。")
                return self._build_ready_chat_session_result(
                    chat_session=current_session,
                    restriction=restriction,
                    shell_seen=shell_seen,
                )

            remaining_ms = max(int((deadline - time.time()) * 1000), 0)
            if remaining_ms <= 0:
                break
            shell = self._locate_chat_shell(
                expected_name=expected_name,
                timeout_ms=remaining_ms,
            )
            if shell:
                shell_seen = True
            refreshed_session = self._refresh_chat_session(
                current_session,
                expected_name=expected_name,
                timeout_ms=remaining_ms,
                allow_generic=False,
            )
            if refreshed_session:
                current_session = refreshed_session
                refreshed_editor, _ = self._extract_chat_session(refreshed_session)
                refreshed_ready_check_ms = max(int((deadline - time.time()) * 1000), 0)
                if self._editor_ready(refreshed_editor, timeout_ms=refreshed_ready_check_ms):
                    waited = time.time() - start
                    log.info(f"{self._prefix()}聊天输入框等待成功，耗时 {waited:.1f} 秒。")
                    return self._build_ready_chat_session_result(
                        chat_session=current_session,
                        shell_seen=shell_seen,
                        ready=True,
                    )

            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(0.12, remaining_seconds), min_seconds=0.0)
        waited = time.time() - start
        log.warning(f"{self._prefix()}聊天输入框等待超时，已等待 {waited:.1f} 秒，等待输入框可见且可写失败")
        return self._build_ready_chat_session_result(
            chat_session=current_session,
            shell_seen=shell_seen,
        )

    def _editor_ready(self, editor, timeout_ms=None):
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(160, min_ms=80)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return False
            visible_timeout_ms = min(self._scale_ms(160, min_ms=80), timeout_ms)
        try:
            if not editor.is_visible(timeout=visible_timeout_ms):
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

    def _find_chat_close_button(self, editor, timeout_ms=None):
        if timeout_ms is None:
            visible_timeout_ms = self._visible_timeout()
            box_timeout_ms = 320
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return None
            visible_timeout_ms = min(self._visible_timeout(), timeout_ms)
            box_timeout_ms = min(320, timeout_ms)
        try:
            editor_box = self._safe_bounding_box(editor, timeout_ms=box_timeout_ms)
            if not editor_box:
                return None

            candidates = []
            for selector in self.CHAT_CLOSE_SELECTORS:
                locator = self.page.locator(selector)
                count = locator.count()
                for idx in range(count - 1, -1, -1):
                    button = locator.nth(idx)
                    try:
                        if not button.is_visible(timeout=visible_timeout_ms):
                            continue
                        box = self._safe_bounding_box(button, timeout_ms=box_timeout_ms)
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

    def _chat_editor_gone(self, editor, timeout_ms=None):
        if not editor:
            return True
        if timeout_ms is None:
            visible_timeout_ms = self._scale_ms(180, min_ms=80)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
            if timeout_ms <= 0:
                return True
            visible_timeout_ms = min(self._scale_ms(180, min_ms=80), timeout_ms)
        try:
            return not editor.is_visible(timeout=visible_timeout_ms)
        except Exception:
            return True

    def _chat_surfaces_cleared(self):
        cleanup_state = self._inspect_surface_cleanup_state()
        if cleanup_state.get("has_chat_close") or cleanup_state.get("has_chat_editor"):
            return False
        return not bool(self._locate_chat_session())

    def _wait_for_chat_surfaces_cleared(self, timeout_ms=None, interval=0.08):
        if timeout_ms is None:
            timeout_ms = self._scale_ms(900, min_ms=360)
        else:
            try:
                timeout_ms = int(timeout_ms)
            except Exception:
                timeout_ms = 0
            timeout_ms = max(timeout_ms, 0)
        if timeout_ms <= 0:
            return self._chat_surfaces_cleared()
        start = time.time()
        deadline = start + timeout_ms / 1000
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
                deadline += paused_seconds
            if self._chat_surfaces_cleared():
                return True
            remaining_seconds = max(deadline - time.time(), 0.0)
            if remaining_seconds <= 0:
                break
            self._sleep(min(interval, remaining_seconds), min_seconds=0.0)
        return self._chat_surfaces_cleared()

    def _chat_window_closed(self, editor=None):
        if not self._chat_editor_gone(editor):
            return False
        return self._chat_surfaces_cleared()

    def _close_chat_button(self, close_button, editor=None, auto_wait_ms=None):
        if auto_wait_ms is None:
            auto_wait_ms = self._scale_ms(1200, min_ms=480)
        else:
            try:
                auto_wait_ms = int(auto_wait_ms)
            except Exception:
                auto_wait_ms = 0
            auto_wait_ms = max(auto_wait_ms, 0)
        if self._chat_window_closed(editor):
            log.info(f"{self._prefix()}聊天窗口已自行关闭，无需额外点击。")
            return 1
        if close_button:
            try:
                self._run_pause_aware_action(
                    lambda step_timeout: close_button.click(timeout=step_timeout),
                    timeout_ms=min(auto_wait_ms, self._scale_ms(1500, min_ms=300)),
                )
                closed = self._wait_for_condition(
                    "聊天窗口收尾",
                    "等待聊天窗口关闭",
                    lambda: self._chat_window_closed(editor),
                    timeout_ms=auto_wait_ms,
                    interval=0.12,
                )
                if closed:
                    log.info(f"{self._prefix()}聊天窗口关闭成功。")
                    return 1
                dismissed = self._dismiss_interfering_popups(
                    context="聊天窗口关闭确认",
                    timeout_ms=auto_wait_ms,
                )
                if dismissed > 0:
                    closed = self._wait_for_condition(
                        "聊天窗口收尾",
                        "等待确认关闭后的聊天窗口关闭",
                        lambda: self._chat_window_closed(editor),
                        timeout_ms=auto_wait_ms,
                        interval=0.12,
                    )
                    if closed:
                        log.info(f"{self._prefix()}聊天窗口确认弹窗已自动确认并关闭成功。")
                        return 1
            except Exception:
                log.warning(f"{self._prefix()}聊天窗口关闭按钮点击失败，改用批量关闭兜底。")
        else:
            auto_gone = self._wait_for_condition(
                "聊天窗口收尾",
                "等待聊天窗口自行关闭",
                lambda: self._chat_window_closed(editor),
                timeout_ms=auto_wait_ms,
                interval=0.12,
            )
            if auto_gone:
                log.info(f"{self._prefix()}聊天窗口已自行关闭。")
                return 1
        closed = self._close_visible_chat_windows(
            settled_timeout_ms=auto_wait_ms,
            idle_timeout_ms=auto_wait_ms,
        )
        if closed > 0:
            log.info(f"{self._prefix()}聊天窗口批量关闭成功，数量={closed}。")
            return closed
        gone_after_fallback = self._wait_for_condition(
            "聊天窗口收尾",
            "等待兜底关闭后的聊天窗口消失",
            lambda: self._chat_window_closed(editor),
            timeout_ms=auto_wait_ms,
            interval=0.12,
        )
        if gone_after_fallback:
            log.info(f"{self._prefix()}聊天窗口已在兜底收尾后关闭。")
            return 1
        return 0
