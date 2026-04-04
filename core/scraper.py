import random
import time
from urllib.parse import parse_qs, urljoin, urlparse

from utils.helpers import run_pause_aware_action, runtime_flag_active, wait_if_runtime_paused
from utils.logger import log


class CollectStopRequested(RuntimeError):
    pass


class Scraper:
    ENV_PAUSE_FLAG = "FB_RPA_PAUSE_FLAG"
    NEWCOMER_SECTION_LABELS = (
        "小组新人",
        "最近加入",
        "新加入的成员",
        "新人加入",
        "新加入团队",
        "New to the group",
        "New members",
        "Recently joined",
        "New member",
        "Recently added",
        "Newcomers",
        "New people",
        "สมาชิกใหม่",
        "สมาชิกใหม่ในกลุ่ม",
        "เพิ่งเข้าร่วม",
        "เข้าร่วมเมื่อไม่นานมานี้",
        "Thành viên mới",
        "Mới tham gia",
        "Vừa tham gia",
        "Người mới trong nhóm",
        "Anggota baru",
        "Baru bergabung",
        "Bergabung baru-baru ini",
        "Ahli baharu",
        "Baru menyertai",
        "Menyertai baru-baru ini",
        "Nuevos miembros",
        "Nuevos en el grupo",
        "Recién incorporados",
        "Miembros recientes",
        "Novos membros",
        "Novo no grupo",
        "Novos no grupo",
        "Ingressou recentemente",
        "Recém-adicionados",
        "Nouveaux membres",
        "Nouveau dans le groupe",
        "Nouveaux dans le groupe",
        "A rejoint récemment",
        "Nouveaux arrivants",
        "Neue Mitglieder",
        "Neu in der Gruppe",
        "Kürzlich beigetreten",
        "Neue in der Gruppe",
        "Nuovi membri",
        "Nuovo nel gruppo",
        "Nuovi nel gruppo",
        "Iscritto di recente",
        "Appena entrato",
        "Nieuwe leden",
        "Nieuw in de groep",
        "Recent toegetreden",
        "Pas toegevoegd",
        "Новые участники",
        "Новый участник",
        "Новые участники группы",
        "Недавно присоединился",
        "Недавно вступил",
        "Nowi członkowie",
        "Nowy w grupie",
        "Nowi w grupie",
        "Dołączył niedawno",
        "Yeni üyeler",
        "Yeni üye",
        "Yeni katıldı",
        "Gruba yeni katılanlar",
        "Grupta yeni",
        "أعضاء جدد",
        "عضو جديد",
        "انضم حديثًا",
        "منضم حديثًا",
        "جدد في المجموعة",
        "नए सदस्य",
        "नया सदस्य",
        "हाल ही में शामिल हुए",
        "समूह में नए",
        "新しいメンバー",
        "新メンバー",
        "最近参加",
        "グループの新しいメンバー",
        "새 멤버",
        "새 회원",
        "최근 가입",
        "그룹 신규 멤버",
        "그룹에 새로 가입함",
        "그룹에 새로 가입한 멤버",
        "សមាជិកថ្មី",
        "ສະມາຊິກໃໝ່",
        "Mga bagong miyembro",
        "Bagong miyembro",
        "Kamakailan lang sumali",
        "గ్రూప్‌లో కొత్త",
        "গ্রুপে নতুন",
        "Mới tham gia nhóm",
        "Baru di grup",
        "Baharu dalam kumpulan",
        "สมาชิกใหม่ของกลุ่ม",
        "ใหม่ในกลุ่ม",
    )
    NEWCOMER_BLOCKED_ROLE_TEXTS = (
        "管理员",
        "管理員",
        "版主",
        "管理和版主",
        "管理员和版主",
        "管理員和版主",
        "管理员及版主",
        "管理員及版主",
        "全明星贡献者",
        "全明星貢獻者",
        "贡献者",
        "貢獻者",
        "公共主页",
        "公開主頁",
        "管理员助理",
        "Admin",
        "Admins",
        "Administrator",
        "Administrators",
        "Moderator",
        "Moderators",
        "Admins and moderators",
        "Group admin",
        "Group admins",
        "Group moderator",
        "Group moderators",
        "Top contributor",
        "Top contributors",
        "Contributor",
        "Contributors",
        "Public page",
        "Public figure",
        "Community contributor",
        "Page ·",
        "Página pública",
        "Página de figura pública",
        "Administradores y moderadores",
        "Administrador",
        "Administradores",
        "Moderador",
        "Moderadores",
        "Colaborador destacado",
        "Colaboradores destacados",
        "Colaborador",
        "Colaboradores",
        "Página oficial",
        "Administradores e moderadores",
        "Administrador do grupo",
        "Moderador do grupo",
        "Principal colaborador",
        "Principais colaboradores",
        "Página pública",
        "Administrateurs et modérateurs",
        "Administrateur",
        "Administrateurs",
        "Modérateur",
        "Modérateurs",
        "Contributeur phare",
        "Contributeurs phares",
        "Contributeur",
        "Contributeurs",
        "Page publique",
        "Page officielle",
        "Admins und Moderatoren",
        "Gruppenadmin",
        "Gruppenadmins",
        "Gruppenmoderator",
        "Gruppenmoderatoren",
        "Öffentliche Seite",
        "Top-Mitwirkender",
        "Mitwirkende",
        "Amministratori e moderatori",
        "Amministratore",
        "Amministratori",
        "Moderatore",
        "Moderatori",
        "Miglior collaboratore",
        "Collaboratore",
        "Pagina pubblica",
        "Pagina ufficiale",
        "Beheerders en moderatoren",
        "Beheerder",
        "Beheerders",
        "Groepsbeheerder",
        "Groepsbeheerders",
        "Groepsmoderator",
        "Groepsmoderatoren",
        "Topbijdrager",
        "Bijdrager",
        "Openbare pagina",
        "Quản trị viên",
        "Quản trị viên và người kiểm duyệt",
        "Người kiểm duyệt",
        "Người đóng góp hàng đầu",
        "Người đóng góp",
        "Trang công khai",
        "Admin dan moderator",
        "Admin grup",
        "Moderator grup",
        "Kontributor teratas",
        "Kontributor",
        "Pentadbir dan moderator",
        "Pentadbir",
        "Pentadbir kumpulan",
        "Moderator kumpulan",
        "Penyumbang teratas",
        "Penyumbang",
        "Halaman awam",
        "অ্যাডমিন",
        "অ্যাডমিন এবং মডারেটর",
        "মডারেটর",
        "ผู้ดูแล",
        "ผู้ดูแลกลุ่ม",
        "ผู้ดูแลและผู้ดูแล",
        "ผู้ร่วมให้ข้อมูล",
        "ผู้มีส่วนร่วม",
        "Halaman publik",
        "Moderator grup",
        "Admin grup",
        "مشرف",
        "مشرفون",
        "المشرفون",
        "مدير",
        "مديرو المجموعة",
        "مساهم بارز",
        "مساهمون بارزون",
        "صفحة عامة",
    )
    GROUP_ENTRY_LABELS = (
        "小组",
        "群组",
        "Groups",
        "Groupes",
        "Grupos",
        "Gruppen",
        "Gruppi",
        "Groepen",
        "Группы",
        "Grupy",
        "Gruplar",
        "المجموعات",
        "समूह",
        "グループ",
        "그룹",
        "Nhóm",
        "Grup",
        "Kumpulan",
        "กลุ่ม",
        "กรุ๊ป",
        "গ্রুপ",
    )
    SEE_ALL_LABELS = (
        "查看全部",
        "See all",
        "View all",
        "Ver todo",
        "Ver todos",
        "Ver tudo",
        "Voir tout",
        "Tout voir",
        "Alle anzeigen",
        "Mostra tutti",
        "Alles bekijken",
        "ดูทั้งหมด",
        "Xem tất cả",
        "Lihat semua",
        "Lihat semuanya",
        "Zobacz wszystko",
        "Hepsini gör",
        "عرض الكل",
        "すべて見る",
        "모두 보기",
        "Посмотреть все",
    )
    JOINED_GROUPS_ENTER_TIMEOUT_SECONDS = 9.0
    JOINED_GROUPS_STABILIZE_TIMEOUT_SECONDS = 6.0
    JOINED_GROUPS_QUICK_STABILIZE_TIMEOUT_SECONDS = 4.0
    GROUPS_DIRECT_NAV_TIMEOUT_MS = 15000
    GROUPS_DIRECT_NAV_SETTLE_TIMEOUT_MS = 2000
    JOINED_GROUP_SECTION_LABELS = (
        "你加入的小组",
        "已加入的小组",
        "你的小组",
        "我的小组",
        "Your groups",
        "Groups you have joined",
        "Groups you've joined",
        "Joined groups",
        "Mes groupes",
        "Tus grupos",
        "Grupos a los que te uniste",
        "Seus grupos",
        "Grupos em que você participa",
        "Vos groupes",
        "Deine Gruppen",
        "I tuoi gruppi",
        "Je groepen",
        "Группы, в которых вы состоите",
        "Twoje grupy",
        "Katıldığın gruplar",
        "المجموعات التي انضممت إليها",
        "जिन समूहों में आप शामिल हैं",
        "参加中のグループ",
        "가입한 그룹",
        "Nhóm của bạn",
        "Các nhóm bạn đã tham gia",
        "Grup Anda",
        "Grup yang Anda ikuti",
        "Kumpulan anda",
        "Kumpulan yang anda sertai",
        "กลุ่มของคุณ",
        "กลุ่มที่คุณเข้าร่วม",
    )
    GROUP_OPEN_ACTION_LABELS = (
        "进入小组",
        "进入群组",
        "查看小组",
        "打开小组",
        "前往小组",
        "进入社团",
        "进入團體",
        "View group",
        "Open group",
        "Go to group",
        "See group",
        "Ver grupo",
        "Abrir grupo",
        "Ir al grupo",
        "Voir le groupe",
        "Ouvrir le groupe",
        "Aller au groupe",
        "Gruppe ansehen",
        "Gruppe öffnen",
        "Zur Gruppe",
        "Visualizza gruppo",
        "Apri gruppo",
        "Vai al gruppo",
        "Xem nhóm",
        "Mở nhóm",
        "Đi tới nhóm",
        "Lihat grup",
        "Buka grup",
        "Pergi ke grup",
        "ดูกลุ่ม",
        "เข้าดูกลุ่ม",
        "เปิดกลุ่ม",
        "ไปที่กลุ่ม",
        "グループを見る",
        "グループを開く",
        "그룹 보기",
        "그룹 열기",
        "عرض المجموعة",
        "فتح المجموعة",
    )
    JOINED_GROUP_SKIP_LABELS = set(
        SEE_ALL_LABELS
        + GROUP_OPEN_ACTION_LABELS
        + (
            "新建小组",
            "Create new group",
            "你的小组",
            "Your groups",
            "发现小组",
            "Discover groups",
        )
    )
    MEMBERS_TAB_LABELS = (
        "Members",
        "People",
        "成员",
        "成员页",
        "成员列表",
        "成员资料",
        "用户",
        "Miembros",
        "Membros",
        "Membres",
        "Mitglieder",
        "Membri",
        "Leden",
        "Участники",
        "Członkowie",
        "Üyeler",
        "الأعضاء",
        "Thành viên",
        "Anggota",
        "Ahli",
        "สมาชิก",
        "สมาชิกในกลุ่ม",
        "ผู้คน",
        "ผู้คนในกลุ่ม",
        "メンバー",
        "멤버",
        "회원",
        "सदस्य",
        "সদস্য",
    )
    MEMBERS_PAGE_HINTS = MEMBERS_TAB_LABELS + (
        "People you may know",
    )
    MEMBERS_TAB_NOISE_TEXTS = (
        "万位成员",
        "查看所有人",
    )
    MEMBER_ACTION_SKIP_LABELS = SEE_ALL_LABELS + (
        "邀请",
        "邀請",
        "Invite",
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
        "发消息",
        "发送消息",
        "發送訊息",
        "發消息",
        "Message",
        "Send message",
        "Send a message",
        "Enviar mensaje",
        "Enviar mensagem",
        "Envoyer un message",
        "Nachricht senden",
        "Invia messaggio",
        "ส่งข้อความ",
        "Nhắn tin",
        "Gửi tin nhắn",
        "Kirim pesan",
        "Hantar mesej",
        "Mesaj gönder",
        "Сообщение",
        "Wyślij wiadomość",
        "メッセージ",
        "메시지",
        "إرسال رسالة",
        "更多",
        "More",
        "Más",
        "Mais",
        "Plus",
        "Mehr",
        "Altro",
        "Meer",
        "เพิ่มเติม",
        "Thêm",
        "Lainnya",
        "Lagi",
        "Więcej",
        "Daha fazla",
        "المزيد",
        "すべて表示",
        "더 보기",
    )
    MEMBER_ACTION_HINT_KEYWORDS = (
        "发消息",
        "发送消息",
        "發送訊息",
        "發消息",
        "message",
        "send message",
        "mensaje",
        "mensagem",
        "envoyer un message",
        "nachricht",
        "messaggio",
        "ส่งข้อความ",
        "ข้อความ",
        "nhắn tin",
        "tin nhắn",
        "pesan",
        "mesej",
        "mesaj",
        "сообщение",
        "wiadomo",
        "メッセージ",
        "메시지",
        "رسالة",
        "বার্তা",
        "मैसेज",
        "add friend",
        "添加好友",
        "加好友",
        "加為好友",
        "follow",
        "关注",
        "關注",
        "追蹤",
        "connect",
        "连接",
        "連接",
        "建立关系",
        "建立關係",
        "invite",
        "邀请",
        "邀請",
        "invitar",
        "convidar",
        "inviter",
        "einladen",
        "invita",
        "เชิญ",
        "mời",
        "undang",
        "jemput",
        "zaproś",
        "davet",
        "دعوة",
    )
    RESERVED_TOP_LEVEL_PATHS = {
        "about",
        "ads",
        "bookmarks",
        "business",
        "community",
        "events",
        "friends",
        "gaming",
        "groups",
        "hashtag",
        "help",
        "latest",
        "login",
        "marketplace",
        "memories",
        "messages",
        "messenger",
        "notifications",
        "pages",
        "people",
        "photo",
        "photos",
        "policies",
        "privacy",
        "profile.php",
        "reel",
        "reels",
        "saved",
        "search",
        "settings",
        "share",
        "stories",
        "story.php",
        "watch",
        "watching",
    }
    GROUP_ROUTE_BLACKLIST = {
        "discover",
        "feed",
        "create",
        "joins",
        "search",
        "notifications",
        "learn",
        "you",
    }

    def __init__(self, page, window_label=None, stop_env=None):
        self.page = page
        self._current_user_id = None
        self.window_label = (window_label or "").strip()
        self.stop_env = str(stop_env or "").strip()
        self._last_collection_block_reason = ""
        self._last_scope_snapshot = None
        self._last_members_ready_snapshot = None
        self._last_members_ready_stable_rounds = 0
        self._last_scope_status = {
            "confirmed": False,
            "reason": "scope_unknown",
            "label": "",
        }
        self._last_member_list_snapshot = {
            "fingerprint": None,
            "visible_count": 0,
            "anchor_found": True,
            "anchor_at_tail": False,
            "scope_confirmed": False,
            "scope_reason": "scope_unknown",
            "scope_label": "",
        }
        self._sequence_anchor_hint = {
            "active": False,
            "anchor_user_id": "",
            "anchor_at_tail": False,
            "anchor_abs_top": None,
        }

    def _prefix(self):
        return f"[{self.window_label}] " if self.window_label else ""

    def _stop_requested(self):
        return bool(self.stop_env and runtime_flag_active(self.stop_env))

    def _raise_if_stop_requested(self):
        if self._stop_requested():
            raise CollectStopRequested("collect_stop_requested")

    def _set_last_collection_block_reason(self, reason=""):
        self._last_collection_block_reason = str(reason or "").strip()

    def get_last_collection_block_reason(self):
        return str(self._last_collection_block_reason or "").strip()

    def _is_login_surface(self, page=None):
        target_page = page or self.page
        if target_page is None:
            return False
        try:
            current_url = str(target_page.url or "").strip().lower()
            if "facebook.com/login" in current_url or "facebook.com/login.php" in current_url:
                return True
        except Exception:
            pass
        try:
            return bool(
                target_page.evaluate(
                    """() => {
                        const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;
                            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                            return true;
                        };
                        const text = normalize((document.body && document.body.innerText) || '');
                        const emailInput = Array.from(document.querySelectorAll('input'))
                            .find((el) => {
                                if (!visible(el)) return false;
                                const type = normalize(el.getAttribute('type') || '');
                                const name = normalize(el.getAttribute('name') || '');
                                const autocomplete = normalize(el.getAttribute('autocomplete') || '');
                                const placeholder = normalize(el.getAttribute('placeholder') || '');
                                return (
                                    type === 'email'
                                    || name === 'email'
                                    || autocomplete === 'username'
                                    || placeholder.includes('email')
                                    || placeholder.includes('phone')
                                    || placeholder.includes('มือถือ')
                                    || placeholder.includes('อีเมล')
                                );
                            });
                        const passwordInput = Array.from(document.querySelectorAll('input[type="password"], input[name="pass"]'))
                            .find((el) => visible(el));
                        const loginButton = Array.from(document.querySelectorAll('button, input[type="submit"], [role="button"], a'))
                            .find((el) => {
                                if (!visible(el)) return false;
                                const label = normalize(
                                    el.innerText
                                    || el.getAttribute('value')
                                    || el.getAttribute('aria-label')
                                    || el.getAttribute('title')
                                    || ''
                                );
                                return (
                                    label === 'log in'
                                    || label === 'login'
                                    || label === 'เข้าสู่ระบบ'
                                    || label.includes('log in')
                                    || label.includes('เข้าสู่ระบบ')
                                );
                            });
                        const createAccountHint =
                            text.includes('create new account')
                            || text.includes('sign up')
                            || text.includes('สมัครใช้งาน')
                            || text.includes('สร้างบัญชีใหม่');
                        return !!passwordInput && (!!emailInput || !!loginButton || createAccountHint);
                    }"""
                )
            )
        except Exception:
            return False

    def _set_last_scope_status(self, confirmed, reason="", label=""):
        self._last_scope_status = {
            "confirmed": bool(confirmed),
            "reason": str(reason or ("ok" if confirmed else "scope_unknown")).strip(),
            "label": str(label or "").strip(),
        }

    def get_last_scope_status(self):
        return dict(self._last_scope_status)

    def _set_last_member_list_snapshot(
        self,
        fingerprint=None,
        visible_count=0,
        anchor_found=True,
        anchor_at_tail=False,
        scope_confirmed=False,
        scope_reason="scope_unknown",
        scope_label="",
    ):
        self._last_member_list_snapshot = {
            "fingerprint": str(fingerprint or "").strip() or None,
            "visible_count": max(0, int(visible_count or 0)),
            "anchor_found": bool(anchor_found),
            "anchor_at_tail": bool(anchor_at_tail),
            "scope_confirmed": bool(scope_confirmed),
            "scope_reason": str(scope_reason or ("ok" if scope_confirmed else "scope_unknown")).strip(),
            "scope_label": str(scope_label or "").strip(),
        }

    def get_last_member_list_snapshot(self):
        return dict(self._last_member_list_snapshot)

    def _get_last_confirmed_newcomers_scope(self):
        snapshot = dict(self._last_scope_snapshot or {})
        if not snapshot.get("confirmed"):
            return None
        return snapshot

    def _can_attempt_newcomers_scope_recovery(self, scope):
        reason = str((scope or {}).get("reason") or "").strip()
        if reason not in {
            "newcomers_label_not_found",
            "newcomers_links_not_found",
            "newcomers_section_not_found",
        }:
            return False
        if not self.is_group_members_page():
            return False
        return self._get_last_confirmed_newcomers_scope() is not None

    def _recover_newcomers_scope_from_last_confirmed(self, failed_scope=None, max_attempts=2):
        cached_scope = self._get_last_confirmed_newcomers_scope()
        if not cached_scope or not self.is_group_members_page():
            return None

        reason = str((failed_scope or {}).get("reason") or "newcomers_scope_unconfirmed").strip()
        cached_top = int(cached_scope.get("sectionTop") or cached_scope.get("headingTop") or 0)
        recovery_positions = []
        if cached_top > 0:
            recovery_positions.extend(
                [
                    max(0, cached_top - 180),
                    max(0, cached_top - 60),
                ]
            )
        recovery_positions.append(0)

        seen_positions = set()
        attempt_index = 0
        for target_y in recovery_positions:
            if target_y in seen_positions:
                continue
            seen_positions.add(target_y)
            attempt_index += 1
            if attempt_index > max_attempts:
                break
            log.info(
                f"{self._prefix()}当前 members 页新人区确认暂时丢失({reason})，"
                f"尝试回到最近一次新人区位置恢复 ({attempt_index}/{max_attempts})。"
            )
            try:
                self.page.evaluate("(y) => window.scrollTo(0, y)", int(target_y))
            except Exception:
                continue
            self._wait_for_scroll_stable(timeout=1.4)
            self._wait_for_member_list_stable(timeout=1.8 if attempt_index == 1 else 2.2)
            recovered_scope = self._locate_newcomers_scope_raw()
            if recovered_scope.get("confirmed"):
                self._last_scope_snapshot = dict(recovered_scope)
                log.info(f"{self._prefix()}新人区临时确认失败后恢复成功，继续留在当前小组。")
                return recovered_scope
        return None

    def _capture_newcomers_search_snapshot(self, scope=None):
        try:
            current_scope = scope if isinstance(scope, dict) else self._locate_newcomers_scope()
            members_snapshot = self._capture_members_page_ready_snapshot() or {}
            page_y = self.page.evaluate("window.scrollY")
            confirmed = bool((current_scope or {}).get("confirmed"))
            reason = str(
                (current_scope or {}).get("reason")
                or ("ok" if confirmed else "newcomers_scope_unconfirmed")
            ).strip()
            label = str((current_scope or {}).get("label") or "").strip()
            scope_container = str((current_scope or {}).get("containerSelector") or "").strip()
            scope_link_count = max(0, int((current_scope or {}).get("linkCount") or 0))
            section_top = float((current_scope or {}).get("sectionTop") or 0)
            section_bottom = float((current_scope or {}).get("sectionBottom") or 0)
            return {
                "scope": current_scope if isinstance(current_scope, dict) else {},
                "confirmed": confirmed,
                "reason": reason,
                "label": label,
                "fingerprint": str(members_snapshot.get("fingerprint") or "").strip() or None,
                "visible_count": max(0, int(members_snapshot.get("visibleCount") or 0)),
                "loading_indicators": max(0, int(members_snapshot.get("loadingIndicators") or 0)),
                "page_y": float(page_y or 0),
                "scope_fingerprint": f"{scope_container}|{label}|{scope_link_count}",
                "section_top": section_top,
                "section_bottom": section_bottom,
            }
        except Exception:
            return {
                "scope": {},
                "confirmed": False,
                "reason": "newcomers_scope_unconfirmed",
                "label": "",
                "fingerprint": None,
                "visible_count": 0,
                "loading_indicators": 0,
                "page_y": 0.0,
                "scope_fingerprint": "",
                "section_top": 0.0,
                "section_bottom": 0.0,
            }

    def _newcomers_search_has_progress(self, previous, current):
        if not previous or not current:
            return True
        if bool(previous.get("confirmed")) != bool(current.get("confirmed")):
            return True
        if str(previous.get("reason") or "") != str(current.get("reason") or ""):
            return True
        if str(previous.get("label") or "") != str(current.get("label") or ""):
            return True
        if str(previous.get("fingerprint") or "") != str(current.get("fingerprint") or ""):
            return True
        if str(previous.get("scope_fingerprint") or "") != str(current.get("scope_fingerprint") or ""):
            return True
        if int(previous.get("visible_count") or 0) != int(current.get("visible_count") or 0):
            return True
        if int(previous.get("loading_indicators") or 0) != int(current.get("loading_indicators") or 0):
            return True
        if abs(float(current.get("section_top") or 0) - float(previous.get("section_top") or 0)) > 8:
            return True
        if abs(float(current.get("section_bottom") or 0) - float(previous.get("section_bottom") or 0)) > 8:
            return True
        return abs(float(current.get("page_y") or 0) - float(previous.get("page_y") or 0)) > 5

    def _wait_for_newcomers_scope_stable(self, timeout=2.4, stable_rounds=2, interval=0.16):
        timeout = max(0.4, float(timeout or 0.4))
        stable_rounds = max(1, int(stable_rounds or 1))
        interval = max(0.08, float(interval or 0.08))
        end_time = time.time() + timeout
        previous = None
        stable = 0
        best_confirmed = None
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            snapshot = self._capture_newcomers_search_snapshot()
            if snapshot.get("confirmed") and int(snapshot.get("loading_indicators") or 0) == 0:
                best_confirmed = snapshot
                if previous and not self._newcomers_search_has_progress(previous, snapshot):
                    stable += 1
                else:
                    stable = 0
                if stable >= stable_rounds - 1:
                    return snapshot
            else:
                stable = 0
            previous = snapshot
            self._pause_aware_sleep(interval)
        return best_confirmed

    def _set_sequence_anchor_hint(self, anchor_user_id="", anchor_at_tail=False, anchor_abs_top=None):
        self._sequence_anchor_hint = {
            "active": bool(anchor_user_id),
            "anchor_user_id": str(anchor_user_id or "").strip(),
            "anchor_at_tail": bool(anchor_at_tail),
            "anchor_abs_top": anchor_abs_top if anchor_abs_top is not None else None,
        }

    def _get_sequence_scroll_plan(self, before_state=None):
        hint = dict(self._sequence_anchor_hint or {})
        if not hint.get("active") or not hint.get("anchor_at_tail"):
            return {
                "container_step_ratio": 0.35,
                "container_min_px": 360,
                "page_step_ratio": 0.35,
                "page_min_px": 360,
            }
        visible_count = max(0, int((before_state or {}).get("visibleCount") or 0))
        if visible_count >= 6:
            step_ratio = 0.16
            min_px = 140
        elif visible_count >= 3:
            step_ratio = 0.14
            min_px = 120
        else:
            step_ratio = 0.12
            min_px = 100
        return {
            "container_step_ratio": step_ratio,
            "container_min_px": min_px,
            "page_step_ratio": step_ratio,
            "page_min_px": min_px,
        }

    def _locate_newcomers_scope_raw(self):
        try:
            scope = self.page.evaluate(
                """(payload) => {
                    const labels = payload.labels || [];
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                            return false;
                        }
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) {
                            return false;
                        }
                        return true;
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
                    const matchesLabel = (text, label) => {
                        if (!text || !label) return false;
                        if (text === label) return true;
                        return [
                            `${label} ·`,
                            `${label}•`,
                            `${label}:`,
                            `${label}：`,
                            `${label}。`,
                            `${label}，`,
                            `${label}（`,
                            `${label} (`,
                            `${label} -`,
                        ].some((prefix) => text.startsWith(prefix));
                    };
                    const getHeadingCandidates = () => {
                        const semanticSelector = 'h1, h2, h3, h4, [role="heading"], [aria-level]';
                        const merged = Array.from(document.querySelectorAll(`${semanticSelector}, div, span`));
                        const deduped = [];
                        const seen = new Set();
                        for (const el of merged) {
                            if (!el || seen.has(el)) continue;
                            seen.add(el);
                            deduped.push(el);
                        }
                        const candidates = [];
                        for (const el of deduped) {
                            if (!visible(el)) continue;
                            const text = normalize(el.innerText || (el.getAttribute && el.getAttribute('aria-label')));
                            if (!text) continue;
                            const matchedLabel = labels.find((label) => matchesLabel(text, label));
                            if (!matchedLabel) continue;
                            if (text.length > matchedLabel.length + 40) continue;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) continue;
                            const style = window.getComputedStyle(el);
                            const weight = parseInt(style && style.fontWeight ? style.fontWeight : '400', 10);
                            const isSemantic = !!(el.matches && el.matches(semanticSelector));
                            candidates.push({
                                element: el,
                                label: matchedLabel,
                                text,
                                rect,
                                score: (isSemantic ? 100 : 0) + (Number.isFinite(weight) ? weight : 0),
                            });
                        }
                        candidates.sort(
                            (a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left || b.score - a.score
                        );
                        return candidates;
                    };

                    const headingCandidates = getHeadingCandidates();
                    const firstMatchedLabel = headingCandidates[0] ? String(headingCandidates[0].label || '') : '';
                    if (!headingCandidates.length) {
                        return {
                            confirmed: false,
                            reason: 'newcomers_label_not_found',
                            label: '',
                        };
                    }

                    const groupUserRe = /\\/groups\\/[^/]+\\/user\\/\\d+\\/?/i;
                    let best = null;
                    let sawMatchedHeading = false;
                    for (const item of headingCandidates) {
                        sawMatchedHeading = true;
                        let node = item.element;
                        for (let depth = 0; depth < 8 && node; depth += 1) {
                            const rect = node.getBoundingClientRect();
                            if (!rect.width || !rect.height) {
                                node = node.parentElement;
                                continue;
                            }
                            if (rect.width < 260 || rect.height < 120) {
                                node = node.parentElement;
                                continue;
                            }

                            let nextHeadingTop = rect.bottom;
                            for (const other of headingCandidates) {
                                if (other.element === item.element) continue;
                                if (other.rect.top <= item.rect.bottom + 12) continue;
                                if (other.rect.left < rect.left - 32 || other.rect.right > rect.right + 32) continue;
                                nextHeadingTop = Math.min(nextHeadingTop, other.rect.top - 12);
                            }
                            const links = Array.from(node.querySelectorAll('a[href*="/groups/"][href*="/user/"]'))
                                .filter((link) => {
                                    if (!visible(link)) return false;
                                    const href = link.getAttribute('href') || '';
                                    if (!groupUserRe.test(href)) return false;
                                    const linkRect = link.getBoundingClientRect();
                                    if (!linkRect.width || !linkRect.height) return false;
                                    if (linkRect.top < item.rect.bottom - 8) return false;
                                    if (linkRect.bottom > nextHeadingTop + 28) return false;
                                    if (linkRect.left < rect.left - 48 || linkRect.right > rect.right + 120) return false;
                                    return true;
                                });
                            if (!links.length) {
                                node = node.parentElement;
                                continue;
                            }

                            const candidate = {
                                confirmed: true,
                                reason: 'ok',
                                label: item.label,
                                headingText: item.text,
                                containerSelector: cssPath(node),
                                headingTop: item.rect.top + window.scrollY,
                                headingBottom: item.rect.bottom + window.scrollY,
                                sectionTop: item.rect.top + window.scrollY,
                                sectionBottom: nextHeadingTop + window.scrollY,
                                sectionLeft: Math.max(0, rect.left),
                                sectionRight: Math.min(window.innerWidth, rect.right),
                                sectionHeight: Math.max(0, nextHeadingTop - item.rect.top),
                                linkCount: links.length,
                                area: Math.max(1, rect.width * Math.max(80, nextHeadingTop - item.rect.top)),
                            };
                            if (
                                !best
                                || candidate.area < best.area
                                || (candidate.area === best.area && candidate.linkCount > best.linkCount)
                            ) {
                                best = candidate;
                            }
                            node = node.parentElement;
                        }
                    }

                    if (!best) {
                        return {
                            confirmed: false,
                            reason: sawMatchedHeading ? 'newcomers_links_not_found' : 'newcomers_section_not_found',
                            label: firstMatchedLabel,
                        };
                    }
                    return best;
                }""",
                {"labels": list(self.NEWCOMER_SECTION_LABELS)},
            )
            return scope
        except Exception as exc:
            return {
                "confirmed": False,
                "reason": f"newcomers_section_lookup_failed:{exc}",
                "label": "",
            }

    def _locate_newcomers_scope(self):
        scope = self._locate_newcomers_scope_raw()
        if isinstance(scope, dict) and scope.get("confirmed"):
            self._last_scope_snapshot = dict(scope)
        return scope

    def _joined_groups_snapshot(self):
        try:
            return self.page.evaluate(
                """(payload) => {
                    const labels = payload.labels || [];
                    const blacklist = payload.blacklist || [];
                    const exactSkips = new Set(payload.skipLabels || []);
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const getVisibleRect = (el) => {
                        if (!el) return null;
                        const inspect = (node) => {
                            if (!node || !(node instanceof Element)) return null;
                            const rect = node.getBoundingClientRect();
                            if (!rect.width || !rect.height) return null;
                            const style = window.getComputedStyle(node);
                            if (!style) return null;
                            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                                return null;
                            }
                            if (rect.bottom < -16 || rect.right < -16 || rect.top > window.innerHeight + 160) {
                                return null;
                            }
                            return rect;
                        };
                        let bestRect = inspect(el);
                        if (bestRect) return bestRect;
                        if (!(el instanceof Element) || !el.querySelectorAll) return null;
                        for (const node of Array.from(el.querySelectorAll('*')).slice(0, 24)) {
                            const rect = inspect(node);
                            if (!rect) continue;
                            if (!bestRect || rect.width * rect.height > bestRect.width * bestRect.height) {
                                bestRect = rect;
                            }
                        }
                        return bestRect;
                    };
                    const visible = (el) => !!getVisibleRect(el);
                    const matchesLabel = (text, label) => {
                        if (!text || !label) return false;
                        if (text === label) return true;
                        return [
                            `${label} ·`,
                            `${label}•`,
                            `${label}:`,
                            `${label}：`,
                            `${label}（`,
                            `${label} (`,
                            `${label} -`,
                        ].some((prefix) => text.startsWith(prefix));
                    };
                    const resolveGroupRoot = (href) => {
                        try {
                            const url = new URL(href, location.origin);
                            const parts = url.pathname.split('/').filter(Boolean);
                            if (parts.length < 2 || parts[0] !== 'groups' || blacklist.includes(parts[1])) {
                                return '';
                            }
                            if (parts.length >= 4 && parts[2] === 'user') {
                                return '';
                            }
                            return `${location.origin}/groups/${parts[1]}`;
                        } catch {
                            return '';
                        }
                    };
                    const extractFallbackText = (groupRoot) => {
                        try {
                            const fallback = decodeURIComponent(String(groupRoot || '').split('/').filter(Boolean).pop() || '');
                            return normalize(fallback.replace(/[-_]+/g, ' '));
                        } catch {
                            return String(groupRoot || '').trim();
                        }
                    };
                    const pickCardContainer = (anchor, rootContainer) => {
                        let node = anchor;
                        let best = rootContainer;
                        for (let depth = 0; depth < 7 && node; depth += 1) {
                            if (!(node instanceof Element) || !rootContainer.contains(node)) break;
                            const rect = getVisibleRect(node);
                            if (!rect) {
                                node = node.parentElement;
                                continue;
                            }
                            if (rect.width >= 220 && rect.height >= 90) {
                                best = node;
                                if (rect.width <= 920 && rect.height <= 520) break;
                            }
                            node = node.parentElement;
                        }
                        return best || rootContainer;
                    };
                    const extractGroupText = (anchor, groupRoot, card) => {
                        const candidates = [];
                        const register = (node, weight = 0) => {
                            if (!node) return;
                            const rect = getVisibleRect(node);
                            if (!rect) return;
                            const text = normalize(
                                node.innerText
                                || (node.getAttribute && (node.getAttribute('aria-label') || node.getAttribute('title')))
                                || ''
                            );
                            if (!text || text.length < 2 || exactSkips.has(text)) return;
                            candidates.push({
                                text,
                                weight,
                                top: rect.top,
                                left: rect.left,
                                length: text.length,
                            });
                        };
                        if (anchor) register(anchor, 260);
                        if (card) {
                            for (const node of Array.from(card.querySelectorAll('a[href], h1, h2, h3, h4, [role="heading"], [aria-level], span, div')).slice(0, 120)) {
                                if (!visible(node)) continue;
                                const sameAnchor = node.closest && node.closest('a[href]');
                                if (sameAnchor) {
                                    const root = resolveGroupRoot(sameAnchor.getAttribute('href') || '');
                                    if (root && root === groupRoot) {
                                        register(node, node.matches && node.matches('a[href]') ? 240 : 210);
                                        continue;
                                    }
                                }
                                if (node.matches && node.matches('h1, h2, h3, h4, [role="heading"], [aria-level]')) {
                                    register(node, 190);
                                    continue;
                                }
                                register(node, 120);
                            }
                        }
                        candidates.sort((a, b) => {
                            const scoreA = a.weight - Math.min(a.length, 160) * 0.35;
                            const scoreB = b.weight - Math.min(b.length, 160) * 0.35;
                            return scoreB - scoreA || a.top - b.top || a.left - b.left;
                        });
                        return candidates.length ? candidates[0].text : extractFallbackText(groupRoot);
                    };
                    const collectMainGroups = (container) => {
                        if (!container || !visible(container)) return [];
                        const items = [];
                        const seen = new Set();
                        for (const a of Array.from(container.querySelectorAll('a[href*="/groups/"]'))) {
                            const href = a.getAttribute('href') || '';
                            const groupRoot = resolveGroupRoot(href);
                            if (!groupRoot) continue;
                            const rect = getVisibleRect(a);
                            if (!rect) continue;
                            if (seen.has(groupRoot)) continue;
                            const card = pickCardContainer(a, container);
                            const text = extractGroupText(a, groupRoot, card);
                            seen.add(groupRoot);
                            items.push({
                                href: groupRoot,
                                text,
                                top: rect.top,
                                left: rect.left,
                            });
                        }
                        items.sort((a, b) => a.top - b.top || a.left - b.left);
                        return items;
                    };
                    const path = location.pathname || '';
                    const onGroupsSurface = path === '/groups' || path === '/groups/' || path.startsWith('/groups/feed') || path.startsWith('/groups/joins');
                    const main = document.querySelector('main, [role="main"]');
                    if (!onGroupsSurface || !main || !visible(main)) {
                        return { ready: false, source: 'not_groups_surface', items: [], empty: false, fingerprint: '' };
                    }

                    const mainItems = collectMainGroups(main);
                    if (mainItems.length >= 1) {
                        return {
                            ready: true,
                            source: 'main_group_roots',
                            items: mainItems,
                            empty: false,
                            fingerprint: mainItems.map((item) => item.href).slice(0, 16).join('|'),
                        };
                    }

                    const mainText = normalize((main && main.innerText) || document.body.innerText || '');
                    const visibleMainLinkCount = Array.from(main.querySelectorAll('a[href]'))
                        .filter((el) => visible(el))
                        .length;
                    const headings = Array.from(main.querySelectorAll('h1, h2, h3, h4, [role="heading"], [aria-level], span, div'))
                        .filter((el) => visible(el))
                        .map((el) => normalize(el.innerText || ''))
                        .filter(Boolean);
                    const joinedSectionHint = headings.some((text) => labels.some((label) => matchesLabel(text, label)));
                    const loadingIndicators = Array.from(
                        document.querySelectorAll(
                            '[role="progressbar"], [aria-busy="true"], [data-visualcompletion="loading-state"]'
                        )
                    ).filter((el) => visible(el)).length;
                    const emptyJoinedSurface =
                        path.startsWith('/groups/joins') &&
                        loadingIndicators === 0 &&
                        mainText.length >= 40 &&
                        (
                            joinedSectionHint
                            || headings.length >= 1
                            || visibleMainLinkCount >= 1
                            || (main.children && main.children.length >= 3)
                        );
                    if (emptyJoinedSurface) {
                        return { ready: true, source: 'empty_joined_surface', items: [], empty: true, fingerprint: 'EMPTY' };
                    }
                    return {
                        ready: false,
                        source: loadingIndicators > 0 ? 'group_roots_loading' : 'group_roots_not_ready',
                        items: [],
                        empty: false,
                        fingerprint: '',
                    };
                }""",
                {
                    "labels": list(self.JOINED_GROUP_SECTION_LABELS),
                    "skipLabels": list(self.JOINED_GROUP_SKIP_LABELS),
                    "blacklist": list(self.GROUP_ROUTE_BLACKLIST),
                },
            )
        except Exception:
            return {"ready": False, "source": "error", "items": [], "empty": False, "fingerprint": ""}

    def _wait_for_condition(self, step_name, detail, checker, timeout=12.0, interval=0.35):
        timeout = max(0.5, float(timeout or 0.5))
        interval = max(0.05, float(interval or 0.05))
        start = time.time()
        log.info(f"{self._prefix()}{step_name}，{detail}...")
        last_error = None
        while time.time() - start < timeout:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                start += paused_seconds
            try:
                if checker():
                    waited = time.time() - start
                    log.info(f"{self._prefix()}{step_name}等待成功，耗时 {waited:.1f} 秒。")
                    return True
            except Exception as exc:
                last_error = exc
            self._pause_aware_sleep(interval)
        waited = time.time() - start
        if last_error:
            log.warning(
                f"{self._prefix()}{step_name}等待超时，已等待 {waited:.1f} 秒，"
                f"{detail}失败: {last_error}"
            )
        else:
            log.warning(
                f"{self._prefix()}{step_name}等待超时，已等待 {waited:.1f} 秒，{detail}失败。"
            )
        return False

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
            stop_env=self.stop_env or None,
            poll_seconds=0.1,
            on_pause=_on_pause,
            on_resume=_on_resume,
        )
        if self._stop_requested():
            raise CollectStopRequested("collect_stop_requested")
        return paused_seconds

    def _run_pause_aware_action(self, action, timeout_ms, slice_ms=320, retry_interval=0.05):
        pause_logged = False

        def _on_pause():
            nonlocal pause_logged
            if not pause_logged:
                log.info("⏸ 已暂停，等待继续...")
                pause_logged = True

        def _on_resume():
            if pause_logged:
                log.info("▶️ 已继续，恢复执行。")

        try:
            return run_pause_aware_action(
                action,
                timeout_ms=timeout_ms,
                pause_env=self.ENV_PAUSE_FLAG,
                stop_env=self.stop_env or None,
                slice_ms=slice_ms,
                retry_interval=retry_interval,
                on_pause=_on_pause,
                on_resume=_on_resume,
            )
        except TimeoutError as exc:
            if str(exc) == "pause_aware_action_stopped" and self.stop_env:
                raise CollectStopRequested("collect_stop_requested") from exc
            raise

    def _goto_pause_aware(self, url, timeout_ms=30000, settle_timeout_ms=3000):
        target_url = str(url or "").strip()
        if not target_url:
            return None
        response = self._run_pause_aware_action(
            lambda step_timeout: self.page.goto(
                target_url,
                wait_until="domcontentloaded",
                timeout=step_timeout,
            ),
            timeout_ms=timeout_ms,
            slice_ms=700,
            retry_interval=0.05,
        )
        if settle_timeout_ms:
            try:
                self._run_pause_aware_action(
                    lambda step_timeout: self.page.wait_for_load_state(
                        "domcontentloaded",
                        timeout=step_timeout,
                    ),
                    timeout_ms=settle_timeout_ms,
                    slice_ms=360,
                    retry_interval=0.05,
                )
            except Exception:
                pass
        return response

    def _go_back_pause_aware(self, timeout_ms=15000):
        before_url = str(self.page.url or "").strip()
        triggered = {"value": False}

        def _step(step_timeout):
            current_url = str(self.page.url or "").strip()
            if before_url and current_url != before_url:
                return True
            if not triggered["value"]:
                triggered["value"] = True
                return self.page.go_back(wait_until="domcontentloaded", timeout=step_timeout)
            self.page.wait_for_load_state("domcontentloaded", timeout=step_timeout)
            current_url = str(self.page.url or "").strip()
            if before_url and current_url == before_url:
                raise TimeoutError("go_back_navigation_not_changed")
            return True

        return self._run_pause_aware_action(
            _step,
            timeout_ms=timeout_ms,
            slice_ms=700,
            retry_interval=0.05,
        )

    def _goto_groups_surface_pause_aware(
        self,
        url,
        timeout_ms=None,
        require_joined=False,
        step_name="小组首页",
    ):
        target_url = str(url or "").strip()
        if not target_url:
            return False, None
        navigation_error = None
        try:
            self._run_pause_aware_action(
                lambda step_timeout: self.page.goto(
                    target_url,
                    wait_until="commit",
                    timeout=step_timeout,
                ),
                timeout_ms=timeout_ms or self.GROUPS_DIRECT_NAV_TIMEOUT_MS,
                slice_ms=700,
                retry_interval=0.05,
            )
        except Exception as exc:
            navigation_error = exc
        ready = self._wait_for_groups_surface_stable(
            timeout=6.0 if require_joined else 5.0,
            stable_rounds=2,
            require_joined=require_joined,
            step_name=step_name,
        )
        return bool(ready), navigation_error

    def _pause_aware_sleep(self, seconds):
        duration = max(0.0, float(seconds or 0.0))
        if duration <= 0:
            return
        self._raise_if_stop_requested()
        deadline = time.time() + duration
        while time.time() < deadline:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                deadline += paused_seconds
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))

    def open_groups_feed(self):
        self._set_last_collection_block_reason("")
        if self._is_login_surface(self.page):
            self._set_last_collection_block_reason("facebook_not_logged_in")
            log.warning(f"{self._prefix()}当前页面命中 Facebook 登录页，停止当前窗口的小组采集。")
            return False
        if self._is_checkpoint_surface(self.page.url):
            log.warning(f"{self._prefix()}当前页面命中 Facebook checkpoint，停止进入小组流程，交由上层统一处理。")
            return False
        if not self._adopt_existing_facebook_page():
            if self._is_login_surface(self.page):
                self._set_last_collection_block_reason("facebook_not_logged_in")
                log.warning(f"{self._prefix()}未登录 Facebook，无法接管有效页面，停止当前窗口的小组采集。")
                return False
            log.warning(f"{self._prefix()}未找到可复用的 Facebook 页面，无法按手动路径进入小组。")
            return False
        if self._is_login_surface(self.page):
            self._set_last_collection_block_reason("facebook_not_logged_in")
            log.warning(f"{self._prefix()}接管页面后识别到 Facebook 登录页，停止当前窗口的小组采集。")
            return False
        if self._is_checkpoint_surface(self.page.url):
            log.warning(f"{self._prefix()}接管页面后命中 Facebook checkpoint，停止进入小组流程，交由上层统一处理。")
            return False
        if self._is_joined_groups_listing_ready():
            log.info(f"{self._prefix()}当前已位于“你的小组”列表，沿用当前页面继续采集。")
            return True
        if self._is_groups_surface(self.page.url):
            log.info(f"{self._prefix()}当前已在小组域页面，先等待当前页面稳定后再继续。")
            ready = self._wait_for_groups_surface_stable(
                timeout=6.0,
                stable_rounds=2,
                require_joined=False,
                step_name="小组首页",
            )
            return bool(ready) or self._is_groups_surface(self.page.url) or self._has_groups_surface_ready()
        if self._open_groups_surface_by_direct_navigation():
            return True
        if self._open_home_surface_for_groups_entry():
            click_info = self._click_groups_entry()
            if click_info and self._wait_for_groups_surface_stable(
                timeout=8.0,
                stable_rounds=2,
                require_joined=False,
                step_name="小组首页",
            ):
                return True
        log.warning(f"{self._prefix()}首页 -> 小组入口链未成功完成，且直达小组页也未成功，停止继续抖动恢复。")
        return False

    def _stabilize_joined_groups_listing(self, timeout=None, step_name="已加入小组列表"):
        if timeout is None:
            timeout = self.JOINED_GROUPS_STABILIZE_TIMEOUT_SECONDS
        self.scroll_to_top()
        self._scroll_joined_groups_container(to_top=True)
        self._warm_up_joined_groups_listing()
        if not self._is_joined_groups_listing_ready():
            return False
        return bool(
            self._wait_for_joined_groups_snapshot_stable(
                timeout=timeout,
                stable_rounds=2,
                require_items=False,
                step_name=step_name,
            )
        )

    def _wait_for_joined_groups_snapshot_stable(
        self,
        timeout=None,
        stable_rounds=2,
        require_items=False,
        step_name="已加入小组列表",
    ):
        if timeout is None:
            timeout = self.JOINED_GROUPS_STABILIZE_TIMEOUT_SECONDS
        timeout = max(0.5, float(timeout or 0.5))
        stable_rounds = max(1, int(stable_rounds or 1))
        end_time = time.time() + timeout
        last_signature = None
        stable = 0
        last_ready_snapshot = None
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            snapshot = self._joined_groups_snapshot() or {}
            if not snapshot.get("ready"):
                last_signature = None
                stable = 0
                self._pause_aware_sleep(0.15)
                continue
            items = list(snapshot.get("items") or [])
            if require_items and not items:
                last_signature = None
                stable = 0
                self._pause_aware_sleep(0.15)
                continue
            signature = (
                str(snapshot.get("fingerprint") or ""),
                bool(snapshot.get("empty")),
                len(items),
            )
            if signature == last_signature:
                stable += 1
            else:
                last_signature = signature
                stable = 0
            last_ready_snapshot = snapshot
            if stable >= stable_rounds - 1:
                return last_ready_snapshot
            self._pause_aware_sleep(0.15)
        return last_ready_snapshot if last_ready_snapshot and not require_items else None

    def _warm_up_joined_groups_listing(self):
        try:
            self.scroll_to_top()
            self._scroll_joined_groups_container(to_top=True)
            self._pause_aware_sleep(0.18)
            if self._has_groups_list_ready():
                return True
            moved = self._scroll_joined_groups_container()
            if moved is None:
                moved = self.scroll_page(step_ratio=0.32, min_px=240, smooth=False)
            if moved:
                self._pause_aware_sleep(0.24)
            self._scroll_joined_groups_container(to_top=True)
            self.scroll_to_top()
            self._pause_aware_sleep(0.12)
        except Exception:
            return False
        return self._has_groups_list_ready()

    def _wait_for_joined_groups_listing_ready(
        self,
        enter_timeout=None,
        stabilize_timeout=None,
        step_name="已加入小组列表",
    ):
        if enter_timeout is None:
            enter_timeout = self.JOINED_GROUPS_ENTER_TIMEOUT_SECONDS
        if stabilize_timeout is None:
            stabilize_timeout = self.JOINED_GROUPS_STABILIZE_TIMEOUT_SECONDS
        entered = self._wait_for_condition(
            step_name,
            "等待进入“你的小组”页面",
            self._is_joined_groups_listing_ready,
            timeout=enter_timeout,
            interval=0.3,
        )
        if not entered:
            return False
        return bool(
            self._wait_for_joined_groups_snapshot_stable(
                timeout=stabilize_timeout,
                stable_rounds=2,
                require_items=False,
                step_name=step_name,
            )
        )

    def collect_joined_groups(self, max_scrolls=8, allowed_group_ids=None, return_meta=False):
        groups = []
        seen_group_ids = set()
        self._set_last_collection_block_reason("")
        normalized_allowed_group_ids = {
            str(group_id or "").strip()
            for group_id in (allowed_group_ids or [])
            if str(group_id or "").strip()
        }
        idle_rounds = 0
        round_idx = 0
        recovery_attempts = 0
        max_recovery_attempts = 2
        unlimited = not max_scrolls or int(max_scrolls) <= 0
        stopped = False

        try:
            self._raise_if_stop_requested()
            if not self.open_groups_feed():
                payload = {
                    "groups": [],
                    "stopped": False,
                    "error": self.get_last_collection_block_reason(),
                }
                return payload if return_meta else []
            if not self._open_joined_groups_listing():
                payload = {
                    "groups": [],
                    "stopped": False,
                    "error": self.get_last_collection_block_reason(),
                }
                return payload if return_meta else []
            self._scroll_joined_groups_container(to_top=True)

            while unlimited or round_idx < int(max_scrolls):
                self._raise_if_stop_requested()
                batch = self._joined_groups_snapshot()
                if not batch.get("ready"):
                    self._wait_for_groups_list_ready(
                        timeout=self.JOINED_GROUPS_STABILIZE_TIMEOUT_SECONDS,
                        step_name="已加入小组列表",
                    )
                    batch = self._joined_groups_snapshot()
                items = list(batch.get("items") or [])
                empty_ready = bool(batch.get("ready")) and not items and bool(batch.get("empty"))
                if empty_ready:
                    log.info(f"{self._prefix()}检测到“你的小组”列表可能为空，开始二次复核。")
                    self._warm_up_joined_groups_listing()
                    rechecked = self._wait_for_joined_groups_snapshot_stable(
                        timeout=self.JOINED_GROUPS_QUICK_STABILIZE_TIMEOUT_SECONDS,
                        stable_rounds=2,
                        require_items=False,
                        step_name="已加入小组列表二次复核",
                    ) or {}
                    rechecked_items = list(rechecked.get("items") or [])
                    if bool(rechecked.get("ready")) and not rechecked_items and bool(rechecked.get("empty")):
                        log.info(f"{self._prefix()}已确认当前“你的小组”列表为空，结束本轮采集。")
                        break
                    batch = rechecked if rechecked else batch
                    items = rechecked_items
                added = 0
                for item in items:
                    group_url = self._extract_group_root_url(item.get("href"))
                    group_id = self._extract_group_id(group_url)
                    if not group_id or group_id in seen_group_ids:
                        continue
                    seen_group_ids.add(group_id)
                    groups.append(
                        {
                            "group_id": group_id,
                            "group_name": str(item.get("text") or "").strip(),
                            "group_url": group_url,
                        }
                    )
                    added += 1

                log.info(
                    f"正在采集已加入小组 (轮次: {round_idx + 1}{'' if unlimited else '/' + str(max_scrolls)})，"
                    f"本轮新增 {added} 个，累计 {len(groups)} 个"
                )
                if normalized_allowed_group_ids and normalized_allowed_group_ids.issubset(seen_group_ids):
                    log.info(f"{self._prefix()}白名单群已全部命中，提前结束已加入小组采集。")
                    break

                idle_rounds = idle_rounds + 1 if added == 0 else 0
                list_ready = bool(items) or self._has_groups_list_ready()
                if added == 0 and idle_rounds >= 2 and not list_ready and recovery_attempts < max_recovery_attempts:
                    recovery_attempts += 1
                    log.info(
                        f"{self._prefix()}已加入小组列表连续为空且列表未稳定，"
                        f"尝试重新进入“你的小组”恢复列表 ({recovery_attempts}/{max_recovery_attempts})。"
                    )
                    if self._open_joined_groups_listing():
                        self._scroll_joined_groups_container(to_top=True)
                        idle_rounds = 0
                        round_idx += 1
                        continue
                self._raise_if_stop_requested()
                moved = self._scroll_joined_groups_container()
                if moved is None:
                    moved = self.scroll_page()
                if not moved and idle_rounds >= 2:
                    break
                if idle_rounds >= 5:
                    break
                round_idx += 1
        except CollectStopRequested:
            stopped = True
            log.warning(f"{self._prefix()}检测到停止指令，结束当前小组采集并保留已获取结果。")

        if return_meta:
            return {
                "groups": groups,
                "stopped": stopped,
                "error": self.get_last_collection_block_reason(),
            }
        return groups

    def collect_joined_group_urls(self, max_scrolls=8, allowed_group_ids=None):
        groups = self.collect_joined_groups(
            max_scrolls=max_scrolls,
            allowed_group_ids=allowed_group_ids,
        )
        return [
            str(item.get("group_url") or "").strip()
            for item in groups
            if str(item.get("group_url") or "").strip()
        ]

    def open_group_members_page(self, group_url):
        group_root = self._extract_group_root_url(group_url)
        if not group_root:
            log.error(f"❌ 无法识别目标小组链接: {group_url}")
            return False

        def finish_members_entry():
            members_ready = self._wait_for_members_page_ready(timeout=24.0)
            if not members_ready:
                log.warning(f"{self._prefix()}成员列表未完成加载，跳过当前小组: {group_url}")
                return False
            current_group_root = self.get_current_group_root_url()
            if current_group_root and current_group_root != group_root:
                log.warning(
                    f"{self._prefix()}当前 members 页归属与目标小组不一致，跳过当前小组: "
                    f"expected={group_root} actual={current_group_root}"
                )
                return False
            if not self.is_group_members_page():
                log.warning(f"{self._prefix()}当前 URL 未明确显示 members，但成员列表已可用: {self.page.url}")
            log.success(f"{self._prefix()}✅ 已进入成员视图: {self.page.url}")
            return True

        current_url = str(self.page.url or "").strip()
        current_group_root = self.get_current_group_root_url()

        if self.is_group_members_page() and current_group_root == group_root:
            return finish_members_entry()

        if self._is_group_root_url(current_url, expected_root=group_root):
            if not self._wait_for_group_page_ready(timeout=18.0):
                log.warning(f"{self._prefix()}目标小组主页未完成加载，跳过当前小组: {group_url}")
                return False
            if not self._open_members_tab(group_root):
                log.warning(f"{self._prefix()}未能通过点击进入成员页，跳过当前小组: {group_url}")
                return False
            return finish_members_entry()

        if self._is_joined_groups_listing_ready():
            if not self._open_target_group_page(group_root):
                log.warning(f"{self._prefix()}未能通过点击进入目标小组主页，跳过当前小组: {group_url}")
                return False
            if not self._wait_for_group_page_ready(timeout=18.0):
                log.warning(f"{self._prefix()}目标小组主页未完成加载，跳过当前小组: {group_url}")
                return False
            if not self._open_members_tab(group_root):
                log.warning(f"{self._prefix()}未能通过点击进入成员页，跳过当前小组: {group_url}")
                return False
            return finish_members_entry()

        if self._is_groups_surface(self.page.url):
            if not self._open_joined_groups_listing():
                log.warning(f"{self._prefix()}未能通过点击进入“你的小组”，跳过当前小组: {group_url}")
                return False
        else:
            if not self.open_groups_feed():
                log.warning(f"{self._prefix()}未能通过点击进入小组首页，跳过当前小组: {group_url}")
                return False
            if not self._open_joined_groups_listing():
                log.warning(f"{self._prefix()}未能通过点击进入“你的小组”，跳过当前小组: {group_url}")
                return False

        if not self._open_target_group_page(group_root):
            log.warning(f"{self._prefix()}未能通过点击进入目标小组主页，跳过当前小组: {group_url}")
            return False
        if not self._wait_for_group_page_ready(timeout=18.0):
            log.warning(f"{self._prefix()}目标小组主页未完成加载，跳过当前小组: {group_url}")
            return False
        if not self._open_members_tab(group_root):
            log.warning(f"{self._prefix()}未能通过点击进入成员页，跳过当前小组: {group_url}")
            return False
        return finish_members_entry()

    def focus_newcomers_section(self, max_scroll_rounds=40):
        try:
            max_rounds = max(1, int(max_scroll_rounds or 1))
        except Exception:
            max_rounds = 40

        self._set_last_scope_status(False, "newcomers_section_not_found")
        if not self._wait_for_members_page_ready(timeout=12.0):
            log.warning(f"{self._prefix()}成员页关键区域未就绪，暂不开始查找“小组新人”。")
            return False
        self.scroll_to_top()
        initial_stable = self._wait_for_member_list_stable(timeout=2.0)
        if initial_stable:
            log.info(f"{self._prefix()}members 页首屏列表已稳定，开始查找“小组新人”。")
        initial_scope = self._wait_for_newcomers_scope_stable(timeout=2.0 if initial_stable else 2.8)
        if initial_scope and initial_scope.get("confirmed"):
            scope = initial_scope.get("scope") or {}
            self.page.evaluate(
                "(y) => window.scrollTo(0, Math.max(0, y - 120))",
                int(scope.get("sectionTop") or scope.get("headingTop") or 0),
            )
            self._wait_for_scroll_stable(timeout=1.8, member_scope=scope)
            self._set_last_scope_status(True, "ok", scope.get("label"))
            log.info(f"{self._prefix()}已稳定定位到 {scope.get('label') or '小组新人'} 区块，从这里开始采集。")
            return True
        last_snapshot = None
        stalled_scroll_rounds = 0
        stagnant_rounds = 0
        for round_index in range(max_rounds):
            search_snapshot = self._capture_newcomers_search_snapshot()
            scope = search_snapshot.get("scope") or {}
            if search_snapshot.get("confirmed"):
                self.page.evaluate(
                    "(y) => window.scrollTo(0, Math.max(0, y - 120))",
                    int(scope.get("sectionTop") or scope.get("headingTop") or 0),
                )
                self._wait_for_scroll_stable(timeout=1.8, member_scope=scope)
                self._set_last_scope_status(True, "ok", scope.get("label"))
                log.info(f"{self._prefix()}已定位到 {scope.get('label') or '小组新人'} 区块，从这里开始采集。")
                return True
            log.info(
                f"{self._prefix()}首屏/当前视图未命中“小组新人”，继续在同一 members 页内平稳下滑搜索"
                f"（第 {round_index + 1}/{max_rounds} 轮）。"
            )
            moved = self.scroll_member_list()
            stable = self._wait_for_member_list_stable(timeout=1.4 if moved else 1.8)
            if stable and moved:
                log.info(f"{self._prefix()}下滑后成员列表已稳定，继续查找“小组新人”。")
            elif stable and not moved:
                log.info(f"{self._prefix()}当前 members 页仍在补充渲染，复查“小组新人”区块。")

            if moved:
                stalled_scroll_rounds = 0
            else:
                stalled_scroll_rounds += 1

            current_snapshot = self._wait_for_newcomers_scope_stable(
                timeout=1.6 if stable else 2.2,
                stable_rounds=2,
                interval=0.16,
            ) or self._capture_newcomers_search_snapshot()
            current_scope = current_snapshot.get("scope") or {}
            if current_snapshot.get("confirmed"):
                self.page.evaluate(
                    "(y) => window.scrollTo(0, Math.max(0, y - 120))",
                    int(current_scope.get("sectionTop") or current_scope.get("headingTop") or 0),
                )
                self._wait_for_scroll_stable(timeout=1.8, member_scope=current_scope)
                self._set_last_scope_status(True, "ok", current_scope.get("label"))
                log.info(
                    f"{self._prefix()}已定位到 {current_scope.get('label') or '小组新人'} 区块，从这里开始采集。"
                )
                return True

            if last_snapshot and not self._newcomers_search_has_progress(last_snapshot, current_snapshot):
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            last_snapshot = current_snapshot

            if not moved and stalled_scroll_rounds >= 2 and stagnant_rounds >= 1:
                break

        final_snapshot = last_snapshot or self._capture_newcomers_search_snapshot()
        final_reason = str(final_snapshot.get("reason") or "newcomers_section_not_found").strip()
        final_label = str(final_snapshot.get("label") or "").strip()
        self._set_last_scope_status(False, final_reason, final_label)
        self._set_last_member_list_snapshot(
            fingerprint=final_snapshot.get("fingerprint"),
            visible_count=final_snapshot.get("visible_count"),
            anchor_found=True,
            anchor_at_tail=False,
            scope_confirmed=False,
            scope_reason=final_reason,
            scope_label=final_label,
        )
        if final_reason == "newcomers_links_not_found":
            log.warning(
                f"{self._prefix()}已命中新人标题，但该区块成员卡片仍未稳定，跳过当前小组。"
            )
        else:
            log.warning(f"{self._prefix()}未能确认“小组新人”区块，跳过当前小组。")
        return False

    def collect_visible_new_member_targets(self, anchor=None, require_after_anchor=False):
        current_user_url = self._get_current_user_url()
        anchor_user_id = anchor.get("user_id") if anchor else None

        payload = self._extract_visible_member_candidates(anchor_user_id)
        items = payload.get("items", []) if isinstance(payload, dict) else (payload or [])
        fallback_items = payload.get("fallbackItems", []) if isinstance(payload, dict) else []
        anchor_found = payload.get("anchorFound", True) if isinstance(payload, dict) else True
        scope_confirmed = bool(payload.get("scopeConfirmed")) if isinstance(payload, dict) else False
        scope_reason = str(payload.get("scopeReason") or "").strip() if isinstance(payload, dict) else ""
        scope_label = str(payload.get("scopeLabel") or "").strip() if isinstance(payload, dict) else ""
        list_fingerprint = str(payload.get("listFingerprint") or "").strip() if isinstance(payload, dict) else ""
        anchor_at_tail = bool(payload.get("anchorAtTail")) if isinstance(payload, dict) else False
        visible_count = int(payload.get("visibleCount") or 0) if isinstance(payload, dict) else 0
        self._set_last_scope_status(
            scope_confirmed,
            scope_reason or ("ok" if scope_confirmed else "newcomers_scope_unconfirmed"),
            scope_label,
        )
        self._set_last_member_list_snapshot(
            fingerprint=list_fingerprint,
            visible_count=visible_count,
            anchor_found=anchor_found,
            anchor_at_tail=anchor_at_tail,
            scope_confirmed=scope_confirmed,
            scope_reason=scope_reason or ("ok" if scope_confirmed else "newcomers_scope_unconfirmed"),
            scope_label=scope_label,
        )

        targets = self._build_member_targets_from_items(items, current_user_url)
        fallback_targets = self._build_member_targets_from_items(fallback_items, current_user_url)

        if require_after_anchor and anchor_user_id and not anchor_found:
            targets = []
        if not scope_confirmed:
            return {
                "targets": [],
                "fallback_targets": [],
                "anchor_found": anchor_found,
            }
        if anchor_user_id and anchor_found:
            self._set_sequence_anchor_hint(
                anchor_user_id=anchor_user_id,
                anchor_at_tail=anchor_at_tail,
                anchor_abs_top=anchor.get("abs_top") if anchor else None,
            )
        elif not anchor_user_id:
            self._set_sequence_anchor_hint()

        return {
            "targets": targets,
            "fallback_targets": fallback_targets,
            "anchor_found": anchor_found,
        }

    def scroll_member_list(self):
        scope = self._locate_newcomers_scope()
        if not scope.get("confirmed") and self._can_attempt_newcomers_scope_recovery(scope):
            recovered_scope = self._recover_newcomers_scope_from_last_confirmed(scope)
            if recovered_scope and recovered_scope.get("confirmed"):
                scope = recovered_scope
        if scope.get("confirmed"):
            before_state = self._capture_member_list_scroll_state(scope)
            scroll_plan = self._get_sequence_scroll_plan(before_state)
            container_result = self._scroll_member_container(
                scope=scope,
                step_ratio=scroll_plan["container_step_ratio"],
                min_px=scroll_plan["container_min_px"],
                smooth=True,
            )
            if container_result.get("found"):
                after_state = self._wait_for_scroll_stable(timeout=2.2, member_scope=scope)
                if self._member_scroll_has_advanced(before_state, after_state):
                    self._adjust_sequence_view_after_scroll(scope)
                    return True
                if not self._should_fallback_to_page_scroll(before_state, after_state):
                    return False
            moved = self.scroll_page(
                step_ratio=scroll_plan["page_step_ratio"],
                min_px=scroll_plan["page_min_px"],
                smooth=True,
                member_scope=scope,
            )
            if moved:
                self._adjust_sequence_view_after_scroll(scope)
            return moved
        return self.scroll_page(step_ratio=0.45, min_px=480, smooth=True)

    def scroll_to_member(self, abs_top=None, offset=160):
        if abs_top is None:
            return
        try:
            target = max(0, int(abs_top) - int(offset))
        except Exception:
            return
        self.page.evaluate("(y) => window.scrollTo(0, y)", target)
        self._wait_for_scroll_stable(timeout=1.5)

    def scroll_page(self, step_ratio=0.85, min_px=900, smooth=False, member_scope=None):
        before_state = self._capture_member_list_scroll_state(member_scope) if member_scope else None
        before = before_state.get("pageY") if before_state else self.page.evaluate("window.scrollY")
        scroll_mode = "smooth" if smooth else "auto"
        self.page.evaluate(
            "(params) => { const { ratio, minPx, mode } = params; const step = Math.max(window.innerHeight * ratio, minPx); window.scrollBy({ top: step, behavior: mode }); }",
            {"ratio": step_ratio, "minPx": min_px, "mode": scroll_mode},
        )
        after_state = self._wait_for_scroll_stable(timeout=2.2, member_scope=member_scope)
        if before_state and after_state:
            return self._member_scroll_has_advanced(before_state, after_state)
        after = after_state.get("pageY") if after_state else self.page.evaluate("window.scrollY")
        return after > before + 5

    def _scroll_member_container(self, scope=None, step_ratio=0.35, min_px=360, smooth=True):
        try:
            scope = scope or self._locate_newcomers_scope()
            if not scope or not scope.get("confirmed"):
                return {"found": False}
            return self.page.evaluate(
                """(params) => {
                    const { ratio, minPx, mode, scopeTop, scopeBottom, scopeLeft, scopeRight } = params;
                    const scrollY = window.scrollY || 0;
                    const scopeTopView = scopeTop - scrollY;
                    const scopeBottomView = scopeBottom - scrollY;
                    const candidates = Array.from(document.querySelectorAll('div, section, ul')).filter((el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        const overflowY = style.overflowY || style.overflow;
                        if (!['auto', 'scroll'].includes(overflowY)) return false;
                        if (el.scrollHeight <= el.clientHeight + 5) return false;
                        if (el.clientHeight < 200) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        if (rect.bottom < scopeTopView - 48 || rect.top > scopeBottomView + 48) return false;
                        if (rect.right < scopeLeft - 48 || rect.left > scopeRight + 120) return false;
                        return true;
                    });
                    if (!candidates.length) return { found: false };
                    candidates.sort((a, b) => {
                        const ar = a.getBoundingClientRect();
                        const br = b.getBoundingClientRect();
                        const ay = Math.abs(ar.top - scopeTopView);
                        const by = Math.abs(br.top - scopeTopView);
                        if (ay !== by) return ay - by;
                        return b.clientHeight - a.clientHeight;
                    });
                    const container = candidates[0];
                    const before = container.scrollTop;
                    const maxTop = Math.max(0, container.scrollHeight - container.clientHeight);
                    const step = Math.max(container.clientHeight * ratio, minPx);
                    container.scrollTo({ top: before + step, behavior: mode });
                    return {
                        found: true,
                        beforeTop: before,
                        maxTop,
                    };
                }""",
                {
                    "ratio": step_ratio,
                    "minPx": min_px,
                    "mode": "smooth" if smooth else "auto",
                    "scopeTop": float(scope.get("headingBottom") or scope.get("sectionTop") or 0),
                    "scopeBottom": float(scope.get("sectionBottom") or 0),
                    "scopeLeft": float(scope.get("sectionLeft") or 0),
                    "scopeRight": float(scope.get("sectionRight") or 0),
                },
            )
        except Exception:
            return {"found": False}

    def _wait_for_scroll_stable(self, timeout=2.0, stable_rounds=3, interval=0.12, member_scope=None):
        end_time = time.time() + max(0.2, timeout)
        if member_scope and member_scope.get("confirmed"):
            last_state = self._capture_member_list_scroll_state(member_scope)
            if not last_state:
                return None
            stable = 0
            while time.time() < end_time:
                paused_seconds = self._wait_if_paused()
                if paused_seconds:
                    end_time += paused_seconds
                self._pause_aware_sleep(interval)
                current_state = self._capture_member_list_scroll_state(member_scope)
                if not current_state:
                    return last_state
                if self._member_scroll_state_is_stable(last_state, current_state):
                    stable += 1
                    if stable >= stable_rounds:
                        return current_state
                else:
                    stable = 0
                last_state = current_state
            return last_state

        try:
            last_y = self.page.evaluate("window.scrollY")
        except Exception:
            return None
        stable = 0
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            self._pause_aware_sleep(interval)
            try:
                current_y = self.page.evaluate("window.scrollY")
            except Exception:
                return None
            if abs(current_y - last_y) <= 1:
                stable += 1
                if stable >= stable_rounds:
                    return {"pageY": current_y}
            else:
                stable = 0
            last_y = current_y
        return {"pageY": last_y}

    def scroll_to_top(self):
        self.page.evaluate("window.scrollTo(0, 0)")
        self._wait_for_scroll_stable(timeout=1.5)

    def _capture_member_list_scroll_state(self, scope=None):
        try:
            current_scope = self._locate_newcomers_scope() if scope is None else scope
            if not current_scope or not current_scope.get("confirmed"):
                return {
                    "scopeConfirmed": False,
                    "scopeReason": str((current_scope or {}).get("reason") or "newcomers_scope_unconfirmed").strip(),
                    "scopeLabel": str((current_scope or {}).get("label") or "").strip(),
                    "pageY": self.page.evaluate("window.scrollY"),
                    "containerFound": False,
                    "containerTop": None,
                    "fingerprint": None,
                    "visibleCount": 0,
                }
            return self.page.evaluate(
                """(scope) => {
                    const groupUserRe = /\\/groups\\/[^/]+\\/user\\/(\\d+)\\/?/i;
                    const pageY = window.scrollY || 0;
                    const sectionTop = Number(scope.sectionTop || 0);
                    const sectionBottom = Number(scope.sectionBottom || 0);
                    const sectionLeft = Number(scope.sectionLeft || 0);
                    const sectionRight = Number(scope.sectionRight || window.innerWidth);
                    const scopeTopView = sectionTop - pageY;
                    const scopeBottomView = sectionBottom - pageY;
                    const laneMinX = Math.max(0, sectionLeft - 32);
                    const laneMaxX = Math.min(window.innerWidth, sectionRight + 96);

                    const candidates = Array.from(document.querySelectorAll('div, section, ul')).filter((el) => {
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        const overflowY = style.overflowY || style.overflow;
                        if (!['auto', 'scroll'].includes(overflowY)) return false;
                        if (el.scrollHeight <= el.clientHeight + 5) return false;
                        if (el.clientHeight < 200) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        if (rect.bottom < scopeTopView - 48 || rect.top > scopeBottomView + 48) return false;
                        if (rect.right < laneMinX || rect.left > laneMaxX) return false;
                        return true;
                    });

                    candidates.sort((a, b) => {
                        const ar = a.getBoundingClientRect();
                        const br = b.getBoundingClientRect();
                        const ay = Math.abs(ar.top - scopeTopView);
                        const by = Math.abs(br.top - scopeTopView);
                        if (ay !== by) return ay - by;
                        return b.clientHeight - a.clientHeight;
                    });
                    const container = candidates[0] || null;
                    const ids = [];
                    const seen = new Set();
                    for (const link of Array.from(document.querySelectorAll('a[href*="/groups/"][href*="/user/"]'))) {
                        const rect = link.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        const absTop = rect.top + pageY;
                        const absBottom = rect.bottom + pageY;
                        if (absTop < sectionTop - 8 || absBottom > sectionBottom + 40) continue;
                        if (rect.right < laneMinX || rect.left > laneMaxX) continue;
                        const href = link.getAttribute('href') || '';
                        const match = href.match(groupUserRe);
                        if (!match || !match[1]) continue;
                        const userId = match[1];
                        if (seen.has(userId)) continue;
                        seen.add(userId);
                        ids.push(userId);
                        if (ids.length >= 12) break;
                    }
                    return {
                        scopeConfirmed: true,
                        scopeReason: 'ok',
                        scopeLabel: String(scope.label || ''),
                        pageY,
                        containerFound: !!container,
                        containerTop: container ? container.scrollTop : null,
                        containerMaxTop: container ? Math.max(0, container.scrollHeight - container.clientHeight) : null,
                        fingerprint: ids.join('|') || null,
                        visibleCount: ids.length,
                    };
                }""",
                current_scope,
            )
        except Exception:
            return None

    def _member_scroll_state_is_stable(self, before, after):
        if not before or not after:
            return False
        if bool(before.get("scopeConfirmed")) != bool(after.get("scopeConfirmed")):
            return False
        if not after.get("scopeConfirmed"):
            return abs(float(after.get("pageY") or 0) - float(before.get("pageY") or 0)) <= 1
        page_stable = abs(float(after.get("pageY") or 0) - float(before.get("pageY") or 0)) <= 1
        if bool(before.get("containerFound")) != bool(after.get("containerFound")):
            return False
        container_stable = True
        if after.get("containerFound"):
            container_stable = abs(float(after.get("containerTop") or 0) - float(before.get("containerTop") or 0)) <= 1
        fingerprint_stable = (before.get("fingerprint") or "") == (after.get("fingerprint") or "")
        visible_count_stable = int(before.get("visibleCount") or 0) == int(after.get("visibleCount") or 0)
        return page_stable and container_stable and fingerprint_stable and visible_count_stable

    def _member_scroll_has_advanced(self, before, after):
        if not before or not after:
            return False
        if not after.get("scopeConfirmed"):
            return False
        if abs(float(after.get("pageY") or 0) - float(before.get("pageY") or 0)) > 5:
            return True
        if after.get("containerFound") and before.get("containerFound"):
            if abs(float(after.get("containerTop") or 0) - float(before.get("containerTop") or 0)) > 5:
                return True
        if (before.get("fingerprint") or "") != (after.get("fingerprint") or ""):
            return True
        return int(before.get("visibleCount") or 0) != int(after.get("visibleCount") or 0)

    def _should_fallback_to_page_scroll(self, before, after):
        if not before or not after:
            return True
        if not after.get("scopeConfirmed"):
            return True
        if not after.get("containerFound"):
            return True
        before_top = float(before.get("containerTop") or 0)
        after_top = float(after.get("containerTop") or 0)
        max_top = after.get("containerMaxTop")
        at_bottom = max_top is not None and abs(float(max_top or 0) - after_top) <= 6
        unchanged = (
            abs(after_top - before_top) <= 1
            and abs(float(after.get("pageY") or 0) - float(before.get("pageY") or 0)) <= 1
            and (before.get("fingerprint") or "") == (after.get("fingerprint") or "")
            and int(before.get("visibleCount") or 0) == int(after.get("visibleCount") or 0)
        )
        return at_bottom or unchanged

    def is_group_members_page(self):
        return self._is_group_members_page(self.page.url)

    def get_current_group_root_url(self):
        return self._extract_group_root_url(self.page.url)

    def get_current_group_id(self):
        parsed = urlparse(urljoin("https://www.facebook.com", self.page.url))
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) >= 2 and segments[0] == "groups":
            return segments[1]
        return None

    def _extract_joined_group_urls(self):
        snapshot = self._joined_groups_snapshot()
        return snapshot.get("items") or []

    def _open_joined_groups_listing(self):
        try:
            if self._is_login_surface(self.page):
                self._set_last_collection_block_reason("facebook_not_logged_in")
                log.warning(f"{self._prefix()}当前页面为 Facebook 登录页，停止当前窗口的小组采集。")
                return False
            if self._is_joined_groups_listing_ready():
                return self._stabilize_joined_groups_listing(step_name="已加入小组列表")
            joins_url = "https://www.facebook.com/groups/joins/"
            navigation_error = None
            current_url = str(self.page.url or "").strip()
            if self._is_joined_groups_surface(current_url):
                log.info(f"{self._prefix()}当前已在“你的小组”链路页面，先等待当前页面稳定后再抓取列表。")
                self._wait_for_groups_surface_stable(
                    timeout=4.5,
                    stable_rounds=2,
                    require_joined=True,
                    step_name="已加入小组列表",
                )
            elif self._is_groups_surface(current_url):
                log.info(f"{self._prefix()}当前仅在小组域页面，等待页面稳定后直接进入“你的小组”页面。")
                self._wait_for_groups_surface_stable(
                    timeout=4.0,
                    stable_rounds=2,
                    require_joined=False,
                    step_name="小组首页",
                )
                ready_after_navigation, navigation_error = self._goto_groups_surface_pause_aware(
                    joins_url,
                    timeout_ms=self.GROUPS_DIRECT_NAV_TIMEOUT_MS,
                    require_joined=True,
                    step_name="已加入小组列表",
                )
                if not ready_after_navigation and navigation_error:
                    current_url = str(self.page.url or "").strip()
                    log.warning(
                        f"{self._prefix()}从小组域切到“你的小组”页面时出现导航异常，且当前页面未稳定 ready："
                        f"url={current_url or '-'} error={navigation_error}"
                    )
            else:
                log.info(f"{self._prefix()}正在直接进入“你的小组”页面。")
                ready_after_navigation, navigation_error = self._goto_groups_surface_pause_aware(
                    joins_url,
                    timeout_ms=self.GROUPS_DIRECT_NAV_TIMEOUT_MS,
                    require_joined=True,
                    step_name="已加入小组列表",
                )
                if not ready_after_navigation and navigation_error:
                    current_url = str(self.page.url or "").strip()
                    log.warning(
                        f"{self._prefix()}直达“你的小组”页面时出现导航异常，且当前页面未稳定 ready："
                        f"url={current_url or '-'} error={navigation_error}"
                    )
            if self._is_joined_groups_surface(self.page.url):
                self._wait_for_groups_surface_stable(
                    timeout=4.5,
                    stable_rounds=2,
                    require_joined=True,
                    step_name="已加入小组列表",
                )
            if self._is_login_surface(self.page):
                self._set_last_collection_block_reason("facebook_not_logged_in")
                log.warning(f"{self._prefix()}进入“你的小组”链路时识别到 Facebook 登录页，停止当前窗口的小组采集。")
                return False
            if self._warm_up_joined_groups_listing():
                return True
            if self._wait_for_joined_groups_listing_ready(step_name="已加入小组列表"):
                return True
        except Exception:
            return False
        return self._is_joined_groups_listing_ready()

    def _ensure_facebook_home_surface(self):
        self._adopt_existing_facebook_page()

    def _safe_page_url(self, page=None):
        target_page = page or self.page
        if target_page is None:
            return ""
        try:
            return str(target_page.url or "").strip()
        except Exception:
            return ""

    def _score_runtime_page_candidate(self, page):
        url = self._safe_page_url(page)
        if not url:
            return -1
        lower_url = url.lower()
        if lower_url.startswith("chrome://"):
            return -1
        if self._is_checkpoint_surface(url):
            return 400
        if self._is_login_surface(page):
            return 80
        if self._is_reusable_facebook_url(url) and self._has_real_facebook_surface(page):
            return 300
        if "facebook.com" in lower_url and not lower_url.startswith("about:blank"):
            return 120
        if lower_url.startswith("about:blank"):
            return 0
        return -1

    def _adopt_existing_facebook_page(self):
        try:
            deadline = time.time() + 12.0
            while time.time() < deadline:
                paused_seconds = self._wait_if_paused()
                if paused_seconds:
                    deadline += paused_seconds
                candidate_pages = []
                try:
                    candidate_pages.append(self.page)
                except Exception:
                    pass
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
                    try:
                        if candidate.is_closed():
                            continue
                    except Exception:
                        continue
                    score = self._score_runtime_page_candidate(candidate)
                    if score > best_score:
                        best_score = score
                        best_page = candidate

                if best_page is not None and best_score >= 300:
                    switched = best_page is not self.page
                    self.page = best_page
                    try:
                        self.page.bring_to_front()
                    except Exception:
                        pass
                    if switched:
                        log.info(f"{self._prefix()}已接管现成 Facebook 页面: {self.page.url}")
                    return True
                self._pause_aware_sleep(0.4)
            log.info(f"{self._prefix()}未发现现成 Facebook 页面，先进入 Facebook 首页。")
            self._goto_pause_aware("https://www.facebook.com/", timeout_ms=30000, settle_timeout_ms=3000)
            self._wait_for_condition(
                "Facebook首页",
                "等待 Facebook 首页真实内容出现",
                lambda: self._score_runtime_page_candidate(self.page) >= 300,
                timeout=12.0,
                interval=0.4,
            )
            return self._score_runtime_page_candidate(self.page) >= 300
        except Exception:
            return False
        return False

    def _is_reusable_facebook_url(self, url):
        current_url = str(url or "").strip()
        return (
            "facebook.com" in current_url
            and "console.bitbrowser.net" not in current_url
            and not current_url.startswith("about:")
        )

    def _is_checkpoint_surface(self, url):
        current_url = str(url or "").strip().lower()
        return "facebook.com/checkpoint" in current_url

    def _has_real_facebook_surface(self, page=None):
        target_page = page or self.page
        try:
            if self._is_checkpoint_surface(target_page.url):
                return False
            if self._is_login_surface(target_page):
                return False
            return bool(
                target_page.evaluate(
                    """() => {
                        const title = (document.title || '').trim();
                        const bodyText = ((document.body && document.body.innerText) || '').trim();
                        const html = ((document.body && document.body.innerHTML) || '').slice(0, 4000);
                        const workbenchLike =
                            title.includes('{{$t(') ||
                            !!document.querySelector('#app .open-info') ||
                            html.includes('chuhai2345') ||
                            html.includes('q-btn') ||
                            html.includes('open-info');
                        if (workbenchLike) return false;
                        if (document.querySelector('[role="banner"]')) return true;
                        if (document.querySelector('[role="main"]')) return true;
                        if (document.querySelector('a[href="/groups/"], a[href*="/groups/?ref=bookmarks"]')) return true;
                        return bodyText.length >= 40;
                    }"""
                )
            )
        except Exception:
            return False

    def _open_home_surface_for_groups_entry(self):
        if self._is_home_surface(self.page.url):
            self._wait_for_condition(
                "Facebook首页",
                "等待“小组”入口出现",
                self._has_groups_sidebar_entry,
                timeout=10.0,
                interval=0.3,
            )
            return self._has_groups_sidebar_entry()
        clicked = self._click_home_entry()
        if not clicked:
            log.info(f"{self._prefix()}未能通过首页入口返回 Facebook 首页，尝试直接回到首页恢复主链。")
            try:
                self._goto_pause_aware("https://www.facebook.com/", timeout_ms=30000, settle_timeout_ms=3000)
            except Exception:
                return False
        self._wait_for_condition(
            "Facebook首页",
            "等待“小组”入口出现",
            self._has_groups_sidebar_entry,
            timeout=12.0,
            interval=0.3,
        )
        return self._has_groups_sidebar_entry()

    def _is_home_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = (parsed.path or "").rstrip("/")
        return path in {"", "/home.php"}

    def _has_groups_sidebar_entry(self):
        try:
            return bool(
                self.page.evaluate(
                    """(tokens) => {
                        const normalizedTokens = (tokens || []).map((token) => (token || '').trim().toLowerCase()).filter(Boolean);
                        const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return false;
                            const style = window.getComputedStyle(el);
                            if (!style) return false;
                            if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                            if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                            return true;
                        };
                        const isGroupsDestination = (href) => {
                            const normalizedHref = String(href || '');
                            if (!normalizedHref) return false;
                            if (/\\/groups\\/(discover|create)(\\/|\\?|$)/i.test(normalizedHref) || /[?&]category=create/i.test(normalizedHref)) return false;
                            if (/\\/groups\\/[0-9A-Za-z_-]+/i.test(normalizedHref)) return false;
                            if (/\\/members(\\/|\\?|$)/i.test(normalizedHref)) return false;
                            return /\\/groups\\/?(\\?|$)/i.test(normalizedHref) || /\\/groups\\/(feed|joins)(\\/|\\?|$)/i.test(normalizedHref);
                        };
                        return Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]')).some((el) => {
                            if (!visible(el)) return false;
                            const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                            const textLower = text.toLowerCase();
                            const href = el.getAttribute('href') || '';
                            const matchesText = normalizedTokens.some((token) => textLower === token || textLower.includes(token));
                            const rect = el.getBoundingClientRect();
                            const sidebarCandidate = rect.left < Math.min(window.innerWidth * 0.38, 420) && rect.top >= 72;
                            const topNavCandidate = rect.top <= 170 && rect.left >= 80 && rect.right <= window.innerWidth - 16;
                            if (matchesText && (sidebarCandidate || topNavCandidate)) return true;
                            return isGroupsDestination(href) && topNavCandidate;
                        });
                    }""",
                    list(self.GROUP_ENTRY_LABELS),
                )
            )
        except Exception:
            return False

    def _click_home_entry(self):
        try:
            log.info(f"{self._prefix()}正在点击 Facebook 首页入口。")
            return self.page.evaluate(
                """(tokens) => {
                    const normalizedTokens = (tokens || []).map((token) => (token || '').trim().toLowerCase()).filter(Boolean);
                    const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const cssPath = (el) => {
                        if (!el || !(el instanceof Element)) return '';
                        const parts = [];
                        let node = el;
                        while (node && node.nodeType === 1 && parts.length < 8) {
                            let part = node.nodeName.toLowerCase();
                            let idx = 1;
                            let sib = node;
                            while ((sib = sib.previousElementSibling)) {
                                if (sib.nodeName === node.nodeName) idx += 1;
                            }
                            part += `:nth-of-type(${idx})`;
                            parts.unshift(part);
                            node = node.parentElement;
                        }
                        return parts.join(' > ');
                    };
                    let best = null;
                    for (const el of Array.from(document.querySelectorAll('a[href], [role="link"], [role="tab"]'))) {
                        if (!visible(el)) continue;
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                        const href = el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        let score = 0;
                        if (href === '/' || href === '') score += 220;
                        if (tokens.some((token) => text === token || text.includes(token))) score += 180;
                        if (rect.top <= 96) score += 90;
                        if (rect.left >= 240 && rect.left <= 560) score += 80;
                        if (rect.left <= 96) score += 40;
                        if (score <= 0) continue;
                        const candidate = {
                            score,
                            clickedText: text,
                            clickedSelector: cssPath(el),
                            el,
                        };
                        if (!best || candidate.score > best.score) best = candidate;
                    }
                    if (!best) return null;
                    best.el.click();
                    return {
                        clickedText: best.clickedText,
                        clickedSelector: best.clickedSelector,
                    };
                }""",
                ["首页", "Home", "Facebook"],
            )
        except Exception:
            return None

    def _click_groups_entry(self):
        try:
            log.info(f"{self._prefix()}正在点击“小组”入口。")
            return self.page.evaluate(
                """(tokens) => {
                    const normalizedTokens = (tokens || []).map((token) => (token || '').trim().toLowerCase()).filter(Boolean);
                    const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const cssPath = (el) => {
                        if (!el || !(el instanceof Element)) return '';
                        const parts = [];
                        let node = el;
                        while (node && node.nodeType === 1 && parts.length < 8) {
                            let part = node.nodeName.toLowerCase();
                            if (node.id) {
                                part += '#' + node.id;
                                parts.unshift(part);
                                break;
                            }
                            let idx = 1;
                            let sib = node;
                            while ((sib = sib.previousElementSibling)) {
                                if (sib.nodeName === node.nodeName) idx += 1;
                            }
                            part += `:nth-of-type(${idx})`;
                            parts.unshift(part);
                            node = node.parentElement;
                        }
                        return parts.join(' > ');
                    };
                    const isGroupsDestination = (href) => {
                        const normalizedHref = String(href || '');
                        if (!normalizedHref) return false;
                        if (/\\/groups\\/(discover|create)(\\/|\\?|$)/i.test(normalizedHref) || /[?&]category=create/i.test(normalizedHref)) return false;
                        if (/\\/groups\\/[0-9A-Za-z_-]+/i.test(normalizedHref)) return false;
                        if (/\\/members(\\/|\\?|$)/i.test(normalizedHref)) return false;
                        return /\\/groups\\/?(\\?|$)/i.test(normalizedHref) || /\\/groups\\/(feed|joins)(\\/|\\?|$)/i.test(normalizedHref);
                    };
                    let best = null;
                    for (const el of Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]'))) {
                        if (!visible(el)) continue;
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                        const textLower = text.toLowerCase();
                        const href = el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        const matchesText = normalizedTokens.some((token) => textLower === token || textLower.includes(token));
                        const looksLikeGroupsHref = isGroupsDestination(href);
                        const sidebarCandidate = rect.left < Math.min(window.innerWidth * 0.38, 420) && rect.top >= 72;
                        const topNavCandidate = rect.top <= 170 && rect.left >= 80 && rect.right <= window.innerWidth - 16;
                        if (!matchesText && !looksLikeGroupsHref) continue;
                        if (!sidebarCandidate && !topNavCandidate) continue;
                        let score = 0;
                        if (/\\/groups\\/feed/i.test(href)) score += 240;
                        if (/\\/groups\\/?(\\?|$)/i.test(href)) score += 200;
                        if (/\\/groups\\/joins/i.test(href)) score += 180;
                        if (/\\/groups\\/(discover|create)/i.test(href) || /[?&]category=create/i.test(href)) score -= 240;
                        if (/\\/groups\\/[0-9A-Za-z_-]+/i.test(href)) score -= 160;
                        if (/\\/members/i.test(href)) score -= 200;
                        if (matchesText) score += 140;
                        if (looksLikeGroupsHref) score += 120;
                        if (sidebarCandidate) score += 90;
                        if (topNavCandidate) score += 120;
                        if (score <= 0) continue;
                        const candidate = {
                            score,
                            clickedText: text,
                            clickedSelector: cssPath(el),
                            el,
                        };
                        if (!best || candidate.score > best.score) best = candidate;
                    }
                    if (!best) return null;
                    best.el.click();
                    return {
                        clickedText: best.clickedText,
                        clickedSelector: best.clickedSelector,
                    };
                }""",
                list(self.GROUP_ENTRY_LABELS),
            )
        except Exception:
            return None

    def _open_target_group_page(self, group_root):
        try:
            log.info(f"{self._prefix()}正在点击目标小组: {group_root}")
            self._scroll_joined_groups_container(to_top=True)
            self._wait_for_joined_groups_snapshot_stable(
                timeout=2.4,
                stable_rounds=2,
                require_items=True,
                step_name="已加入小组列表",
            )
            last_fingerprint = None
            idle_rounds = 0
            for _ in range(6):
                before_url = self.page.url
                clicked = self.page.evaluate(
                    """(payload) => {
                    const targetGroupRoot = String(payload?.targetGroupRoot || '').trim().replace(/\\/+$/, '');
                    const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const cssPath = (el) => {
                        if (!el || !(el instanceof Element)) return '';
                        const parts = [];
                        let node = el;
                        while (node && node.nodeType === 1 && parts.length < 8) {
                            let part = node.nodeName.toLowerCase();
                            let idx = 1;
                            let sib = node;
                            while ((sib = sib.previousElementSibling)) {
                                if (sib.nodeName === node.nodeName) idx += 1;
                            }
                            part += `:nth-of-type(${idx})`;
                            parts.unshift(part);
                            node = node.parentElement;
                        }
                        return parts.join(' > ');
                    };
                    const main = document.querySelector('main, [role="main"]');
                    const mainRect = main ? main.getBoundingClientRect() : null;
                    const inMainContent = (rect) => {
                        if (!rect || !mainRect) return false;
                        return (
                            rect.left >= mainRect.left + 24 &&
                            rect.right <= mainRect.right + 12 &&
                            rect.top >= mainRect.top - 8 &&
                            rect.bottom <= mainRect.bottom + 12
                        );
                    };
                    const classifyGroupHref = (href) => {
                        try {
                            const url = new URL(href, location.origin);
                            const parts = url.pathname.split('/').filter(Boolean);
                            if (parts.length >= 2 && parts[0] === 'groups') {
                                const root = `https://www.facebook.com/groups/${parts[1]}`;
                                if (root !== targetGroupRoot) {
                                    return { type: 'other_group', root };
                                }
                                if (parts.length === 2) {
                                    return { type: 'group_root', root };
                                }
                                if (parts.length >= 4 && parts[2] === 'user') {
                                    return { type: 'member_profile', root };
                                }
                                if (parts.length >= 3 && ['posts', 'permalink'].includes(parts[2])) {
                                    return { type: 'post', root };
                                }
                                if (parts.length >= 3 && ['media', 'about', 'members', 'events', 'files', 'photos', 'albums', 'guides', 'buy_sell_discussion', 'learning_content'].includes(parts[2])) {
                                    return { type: 'subpage', root };
                                }
                                return { type: 'non_root_subpath', root };
                            }
                        } catch (e) {}
                        return { type: 'non_group', root: '' };
                    };
                    const path = location.pathname || '';
                    const onJoinedSurface =
                        path === '/groups' ||
                        path === '/groups/' ||
                        /^\\/groups\\/(joins|feed)(\\/|\\?|$)/i.test(path);
                    if (onJoinedSurface && main) {
                        const rootAnchors = Array.from(main.querySelectorAll('a[href]')).filter((el) => {
                            if (!visible(el)) return false;
                            const href = el.getAttribute('href') || '';
                            const classified = classifyGroupHref(href);
                            return classified.root === targetGroupRoot && classified.type === 'group_root';
                        });
                        const seenCards = new Set();
                        const cardCandidates = [];
                        for (const anchor of rootAnchors) {
                            let node = anchor;
                            for (let depth = 0; depth < 7 && node; depth += 1) {
                                if (!(node instanceof Element)) break;
                                const rect = node.getBoundingClientRect();
                                if (
                                    rect.width >= 260 &&
                                    rect.height >= 120 &&
                                    inMainContent(rect)
                                ) {
                                    const key = cssPath(node);
                                    if (!seenCards.has(key)) {
                                        seenCards.add(key);
                                        cardCandidates.push({ node, rect });
                                    }
                                }
                                node = node.parentElement;
                            }
                        }
                        cardCandidates.sort((a, b) => a.rect.top - b.rect.top || a.rect.left - b.rect.left);
                        for (const card of cardCandidates) {
                            const primaryAnchor = rootAnchors
                                .map((el) => ({
                                    el,
                                    rect: el.getBoundingClientRect(),
                                    text: normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || ''),
                                    href: el.getAttribute('href') || '',
                                    selector: cssPath(el),
                                    hasText: normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '').length >= 2,
                                }))
                                .filter((item) => card.node.contains(item.el) && inMainContent(item.rect))
                                .sort((a, b) => Number(b.hasText) - Number(a.hasText) || a.rect.top - b.rect.top || a.rect.left - b.rect.left)[0];
                            if (primaryAnchor) {
                                primaryAnchor.el.click();
                                return {
                                    candidateCount: 1,
                                    clickedUrl: new URL(primaryAnchor.href, location.origin).href,
                                    clickedType: 'group_root',
                                    clickedText: primaryAnchor.text,
                                    clickedSelector: primaryAnchor.selector,
                                };
                            }
                        }
                    }
                    const matches = [];
                    const rejected = [];
                    for (const el of Array.from(document.querySelectorAll('a[href]'))) {
                        if (!visible(el)) continue;
                        const href = el.getAttribute('href') || '';
                        const classified = classifyGroupHref(href);
                        if (classified.root !== targetGroupRoot) continue;
                        const rect = el.getBoundingClientRect();
                        if (classified.type !== 'group_root') {
                            rejected.push({
                                href: new URL(href, location.origin).href,
                                type: classified.type,
                                text: normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || ''),
                            });
                            continue;
                        }
                        matches.push({
                            top: rect.top,
                            left: rect.left,
                            clickedUrl: new URL(href, location.origin).href,
                            clickedType: classified.type,
                            clickedText: normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || ''),
                            clickedSelector: cssPath(el),
                            el,
                        });
                    }
                    matches.sort((a, b) => a.top - b.top || a.left - b.left);
                    const best = matches[0];
                    if (!best) return null;
                    best.el.click();
                    return {
                        candidateCount: matches.length,
                        rejected: rejected.slice(0, 8),
                        clickedUrl: best.clickedUrl,
                        clickedType: best.clickedType,
                        clickedText: best.clickedText,
                        clickedSelector: best.clickedSelector,
                    };
                }""",
                    {
                        "targetGroupRoot": group_root,
                    },
                )
                if clicked:
                    log.info(
                        f"{self._prefix()}目标小组候选命中: "
                        f"url={clicked.get('clickedUrl') or ''} "
                        f"type={clicked.get('clickedType') or ''} "
                        f"text={clicked.get('clickedText') or ''}"
                    )
                    self._wait_for_group_navigation(before_url, group_root, timeout=2.5)
                    current_url = str(self.page.url or "").strip()
                    if self._is_group_root_url(current_url, expected_root=group_root):
                        return True
                    if current_url.startswith(group_root + "/user/"):
                        log.warning(f"{self._prefix()}目标小组点击误入成员 profile，当前 URL: {current_url}")
                    elif current_url.startswith(group_root + "/"):
                        log.warning(f"{self._prefix()}目标小组点击误入子页面，当前 URL: {current_url}")
                    else:
                        log.warning(f"{self._prefix()}目标小组点击后未进入 group root，当前 URL: {current_url}")
                    direct_url = str(clicked.get("clickedUrl") or group_root or "").strip()
                    if direct_url:
                        try:
                            log.info(f"{self._prefix()}目标小组点击未生效，尝试直接进入已命中的目标小组: {direct_url}")
                            self._goto_pause_aware(direct_url, timeout_ms=30000, settle_timeout_ms=3000)
                            self._wait_for_group_navigation(current_url, group_root, timeout=2.0)
                            current_url = str(self.page.url or "").strip()
                            if self._is_group_root_url(current_url, expected_root=group_root):
                                return True
                        except Exception:
                            pass
                    try:
                        self._go_back_pause_aware(timeout_ms=15000)
                    except Exception:
                        pass
                    self._wait_for_joined_groups_snapshot_stable(
                        timeout=2.0,
                        stable_rounds=1,
                        require_items=True,
                        step_name="已加入小组列表",
                    )
                snapshot = self._joined_groups_snapshot()
                fingerprint = "|".join(item.get("href") or "" for item in (snapshot.get("items") or [])[:12])
                idle_rounds = idle_rounds + 1 if fingerprint == last_fingerprint else 0
                moved = self._scroll_joined_groups_container()
                if moved is None:
                    moved = self.scroll_page(step_ratio=0.6, min_px=420, smooth=False)
                if not moved and idle_rounds >= 1:
                    break
                last_fingerprint = fingerprint
                self._wait_for_joined_groups_snapshot_stable(
                    timeout=1.8,
                    stable_rounds=1,
                    require_items=True,
                    step_name="已加入小组列表",
                )
            return False
        except Exception:
            return False

    def _open_members_tab(self, expected_group_root=None):
        try:
            log.info(f"{self._prefix()}正在点击“用户 / Members / 成员”tab。")
            before_url = self.page.url
            expected_group_root = str(expected_group_root or "").strip().rstrip("/")
            for _ in range(4):
                clicked = self.page.evaluate(
                    """(payload) => {
                    const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                    const tokens = Array.isArray(payload?.tokens)
                        ? payload.tokens.map((token) => normalize(token).toLowerCase()).filter(Boolean)
                        : [];
                    const expectedGroupRoot = String(payload?.expectedGroupRoot || '').trim().replace(/\\/+$/, '');
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const cssPath = (el) => {
                        if (!el || !(el instanceof Element)) return '';
                        const parts = [];
                        let node = el;
                        while (node && node.nodeType === 1 && parts.length < 8) {
                            let part = node.nodeName.toLowerCase();
                            let idx = 1;
                            let sib = node;
                            while ((sib = sib.previousElementSibling)) {
                                if (sib.nodeName === node.nodeName) idx += 1;
                            }
                            part += `:nth-of-type(${idx})`;
                            parts.unshift(part);
                            node = node.parentElement;
                        }
                        return parts.join(' > ');
                    };
                    const resolveGroupRoot = (href) => {
                        try {
                            const url = new URL(href || '', location.origin);
                            const parts = url.pathname.split('/').filter(Boolean);
                            if (parts.length >= 2 && parts[0] === 'groups') {
                                return `https://www.facebook.com/groups/${parts[1]}`;
                            }
                        } catch (e) {}
                        return '';
                    };
                    let best = null;
                    for (const el of Array.from(document.querySelectorAll('a[href], [role="tab"], div[role="link"], span[role="link"]'))) {
                        if (!visible(el)) continue;
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                        const textLower = text.toLowerCase();
                        const href = el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        const groupRoot = resolveGroupRoot(href);
                        const hasMembersHref = /\\/members(\\/|\\?|$)/i.test(href);
                        const hasMembersText = tokens.some((token) => textLower === token || textLower.includes(token));
                        if (!hasMembersHref && !hasMembersText) continue;
                        if (expectedGroupRoot && groupRoot && groupRoot !== expectedGroupRoot) continue;
                        let score = 0;
                        if (hasMembersHref) score += 220;
                        if (hasMembersText) score += 180;
                        if (expectedGroupRoot && groupRoot === expectedGroupRoot) score += 180;
                        if (rect.y >= 210 && rect.y <= 340) score += 80;
                        if (rect.y < 190) score -= 120;
                        if ((payload.membersTabNoiseTexts || []).some((token) => text.includes(token)) && rect.y < 220) score -= 160;
                        if (score <= 0) continue;
                        const candidate = {
                            score,
                            clickedText: text,
                            clickedHref: href,
                            clickedSelector: cssPath(el),
                            el,
                        };
                        if (!best || candidate.score > best.score) best = candidate;
                    }
                    if (!best) return null;
                    best.el.click();
                    return {
                        clickedText: best.clickedText,
                        clickedHref: best.clickedHref,
                        clickedSelector: best.clickedSelector,
                    };
                }""",
                    {
                        "tokens": list(self.MEMBERS_TAB_LABELS),
                        "membersTabNoiseTexts": list(self.MEMBERS_TAB_NOISE_TEXTS),
                        "expectedGroupRoot": expected_group_root,
                    },
                )
                if clicked:
                    if self._wait_for_members_tab_activation(
                        before_url,
                        expected_group_root=expected_group_root,
                        timeout=3.0,
                    ):
                        return True
                self._pause_aware_sleep(0.5)
            return False
        except Exception:
            return False

    def _wait_for_members_tab_activation(self, previous_url, expected_group_root=None, timeout=3.0, interval=0.15):
        previous = str(previous_url or "").strip()
        expected_group_root = str(expected_group_root or "").strip().rstrip("/")
        end_time = time.time() + max(0.5, float(timeout or 0.5))
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            current_url = str(self.page.url or "").strip()
            current_group_root = self._extract_group_root_url(current_url)
            if current_url != previous and "/members" in current_url:
                if not expected_group_root or current_group_root == expected_group_root:
                    remaining = max(0.4, end_time - time.time())
                    if self._wait_for_members_page_ready(timeout=min(remaining, 4.0)):
                        return True
            if (not expected_group_root or current_group_root == expected_group_root) and self._has_members_page_ready():
                return True
            self._pause_aware_sleep(interval)
        return False

    def _wait_for_group_navigation(self, previous_url, group_root, timeout=2.5, interval=0.15):
        previous = str(previous_url or "").strip()
        target_root = str(group_root or "").strip().rstrip("/")
        end_time = time.time() + max(0.5, float(timeout or 0.5))
        last_url = str(self.page.url or "").strip()
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            current_url = str(self.page.url or "").strip()
            if current_url != previous:
                return current_url
            if target_root and (
                current_url == target_root or current_url.startswith(target_root + "/")
            ):
                return current_url
            last_url = current_url
            self._pause_aware_sleep(interval)
        return last_url

    def _is_groups_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = parsed.path or ""
        return path in {"/groups", "/groups/"} or path.startswith("/groups/feed") or path.startswith("/groups/joins")

    def _is_joined_groups_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = parsed.path or ""
        return path.startswith("/groups/joins")

    def _capture_groups_surface_snapshot(self):
        try:
            return self.page.evaluate(
                """() => {
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const path = location.pathname || '';
                    const onGroupsSurface =
                        path === '/groups' ||
                        path === '/groups/' ||
                        path.startsWith('/groups/feed') ||
                        path.startsWith('/groups/joins');
                    const onJoinedSurface = path.startsWith('/groups/joins');
                    const main = document.querySelector('main, [role="main"]');
                    const mainVisible = !!(main && visible(main));
                    const mainRect = mainVisible ? main.getBoundingClientRect() : null;
                    const loadingIndicators = Array.from(
                        document.querySelectorAll('[role="progressbar"], [aria-busy="true"], [data-visualcompletion="loading-state"]')
                    ).filter((el) => visible(el)).length;
                    const isGroupRootHref = (href) => {
                        const normalizedHref = String(href || '');
                        if (!normalizedHref) return false;
                        if (/\\/groups\\/(discover|create)(\\/|\\?|$)/i.test(normalizedHref) || /[?&]category=create/i.test(normalizedHref)) {
                            return false;
                        }
                        if (/\\/members(\\/|\\?|$)/i.test(normalizedHref)) return false;
                        return /\\/groups\\/[0-9A-Za-z_-]+(\\/|\\?|$)/i.test(normalizedHref);
                    };
                    const groupRoots = [];
                    if (mainVisible) {
                        const seen = new Set();
                        for (const el of Array.from(main.querySelectorAll('a[href]'))) {
                            if (!visible(el)) continue;
                            const href = (el.getAttribute && (el.getAttribute('href') || '')) || '';
                            if (!isGroupRootHref(href)) continue;
                            try {
                                const url = new URL(href, location.origin);
                                const parts = url.pathname.split('/').filter(Boolean);
                                if (parts.length >= 2 && parts[0] === 'groups') {
                                    const root = `${location.origin}/groups/${parts[1]}`;
                                    if (!seen.has(root)) {
                                        seen.add(root);
                                        groupRoots.push(root);
                                    }
                                }
                            } catch (e) {}
                        }
                    }
                    const headings = mainVisible
                        ? Array.from(main.querySelectorAll('h1, h2, h3, h4, [role="heading"], [aria-level]'))
                            .filter((el) => visible(el))
                            .map((el) => normalize(el.innerText || ''))
                            .filter(Boolean)
                        : [];
                    const mainText = mainVisible ? normalize(main.innerText || '') : '';
                    const ready =
                        onGroupsSurface &&
                        mainVisible &&
                        loadingIndicators === 0 &&
                        (
                            groupRoots.length >= 2 ||
                            (groupRoots.length >= 1 && mainText.length >= 120) ||
                            (headings.length >= 2 && mainText.length >= 180)
                        );
                    return {
                        onGroupsSurface,
                        onJoinedSurface,
                        mainVisible,
                        mainHeight: mainRect ? mainRect.height : 0,
                        loadingIndicators,
                        groupRootCount: groupRoots.length,
                        headingCount: headings.length,
                        mainTextLength: mainText.length,
                        fingerprint: groupRoots.slice(0, 12).join('|'),
                        ready,
                    };
                }"""
            )
        except Exception:
            return None

    def _wait_for_groups_surface_stable(
        self,
        timeout=6.0,
        stable_rounds=2,
        require_joined=False,
        step_name="小组首页",
    ):
        timeout = max(0.5, float(timeout or 0.5))
        stable_rounds = max(1, int(stable_rounds or 1))
        end_time = time.time() + timeout
        last_signature = None
        stable = 0
        last_ready_snapshot = None
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            snapshot = self._capture_groups_surface_snapshot() or {}
            if not snapshot.get("ready"):
                last_signature = None
                stable = 0
                self._pause_aware_sleep(0.15)
                continue
            if require_joined and not snapshot.get("onJoinedSurface"):
                last_signature = None
                stable = 0
                self._pause_aware_sleep(0.15)
                continue
            signature = (
                bool(snapshot.get("onJoinedSurface")),
                int(snapshot.get("groupRootCount") or 0),
                int(snapshot.get("headingCount") or 0),
                str(snapshot.get("fingerprint") or ""),
            )
            if signature == last_signature:
                stable += 1
            else:
                last_signature = signature
                stable = 0
            last_ready_snapshot = snapshot
            if stable >= stable_rounds - 1:
                return last_ready_snapshot
            self._pause_aware_sleep(0.15)
        return last_ready_snapshot

    def _is_joined_groups_listing_ready(self):
        if not self._is_groups_surface(self.page.url):
            return False
        return self._has_groups_list_ready()

    def _has_groups_surface_ready(self):
        if self._is_checkpoint_surface(self.page.url):
            return False
        try:
            snapshot = self._capture_groups_surface_snapshot() or {}
            return bool(snapshot.get("ready"))
        except Exception:
            return False

    def _wait_for_groups_surface(self, timeout=12.0, step_name="小组首页"):
        return self._wait_for_condition(
            step_name,
            "等待进入小组页面",
            self._has_groups_surface_ready,
            timeout=timeout,
            interval=0.3,
        )

    def _wait_for_groups_list_ready(self, timeout=4.0, step_name="小组列表"):
        return self._wait_for_condition(
            step_name,
            "等待小组列表出现",
            self._has_groups_list_ready,
            timeout=timeout,
            interval=0.3,
        )

    def _wait_for_group_page_ready(self, timeout=18.0):
        return self._wait_for_condition(
            "目标小组主页",
            "等待群组页面主区域出现",
            self._has_group_page_ready,
            timeout=timeout,
            interval=0.35,
        )

    def _wait_for_members_page_ready(self, timeout=24.0):
        self._last_members_ready_snapshot = None
        self._last_members_ready_stable_rounds = 0
        self._set_last_scope_status(False, "scope_pending", "")
        self._set_last_member_list_snapshot(
            fingerprint=None,
            visible_count=0,
            anchor_found=True,
            anchor_at_tail=False,
            scope_confirmed=False,
            scope_reason="members_page_not_ready",
            scope_label="",
        )
        return self._wait_for_condition(
            "小组成员页",
            "等待成员列表区域出现",
            self._has_members_page_ready,
            timeout=timeout,
            interval=0.35,
        )

    def _has_groups_list_ready(self):
        try:
            snapshot = self._joined_groups_snapshot()
            return bool(snapshot.get("ready")) and (bool(snapshot.get("items")) or bool(snapshot.get("empty")))
        except Exception:
            return False

    def _open_groups_surface_by_direct_navigation(self):
        joins_url = "https://www.facebook.com/groups/joins/"
        current_url = str(self.page.url or "").strip()
        try:
            if self._is_groups_surface(current_url):
                log.info(f"{self._prefix()}当前已处于小组域页面，先等待当前页面稳定。")
                return bool(
                    self._wait_for_groups_surface_stable(
                        timeout=5.0,
                        stable_rounds=2,
                        require_joined=False,
                        step_name="小组首页",
                    )
                )
            log.warning(f"{self._prefix()}“小组”入口识别失败，尝试直接进入: {joins_url}")
            ready_after_navigation, navigation_error = self._goto_groups_surface_pause_aware(
                joins_url,
                timeout_ms=self.GROUPS_DIRECT_NAV_TIMEOUT_MS,
                require_joined=False,
                step_name="小组首页",
            )
            if not ready_after_navigation and navigation_error:
                current_url = str(self.page.url or "").strip()
                log.warning(
                    f"{self._prefix()}直接进入 {joins_url} 时出现导航异常，且当前页面未稳定 ready："
                    f"url={current_url or '-'} error={navigation_error}"
                )
            if ready_after_navigation:
                return True
            if self._is_groups_surface(self.page.url):
                return True
            feed_url = "https://www.facebook.com/groups/feed/"
            log.warning(f"{self._prefix()}joins 页面未稳定 ready，尝试回退进入: {feed_url}")
            ready_after_navigation, navigation_error = self._goto_groups_surface_pause_aware(
                feed_url,
                timeout_ms=self.GROUPS_DIRECT_NAV_TIMEOUT_MS,
                require_joined=False,
                step_name="小组首页",
            )
            if not ready_after_navigation and navigation_error:
                current_url = str(self.page.url or "").strip()
                log.warning(
                    f"{self._prefix()}直接进入 {feed_url} 时出现导航异常，且当前页面未稳定 ready："
                    f"url={current_url or '-'} error={navigation_error}"
                )
            if ready_after_navigation:
                return True
        except Exception:
            pass
        return False

    def _has_group_page_ready(self):
        try:
            return bool(
                self.page.evaluate(
                    """(payload) => {
                        const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                        const tokens = (payload.tokens || []).map((token) => normalize(token).toLowerCase()).filter(Boolean);
                        const path = location.pathname || '';
                        if (!path.startsWith('/groups/') || path.includes('/members')) return false;
                        const main = document.querySelector('[role="main"]');
                        if (main && main.getBoundingClientRect().height > 200) return true;
                        const membersLink = Array.from(document.querySelectorAll('a[href], [role="tab"], div[role="link"], span[role="link"]'))
                            .find((el) => {
                                const href = el.getAttribute && (el.getAttribute('href') || '');
                                const text = normalize(el.innerText || el.getAttribute('aria-label') || '');
                                const textLower = text.toLowerCase();
                                const rect = el.getBoundingClientRect();
                                if (!rect.width || !rect.height) return false;
                                if (rect.bottom < 0 || rect.top > window.innerHeight) return false;
                                if (/\\/members(\\/|\\?|$)/.test(href)) return true;
                                return tokens.some((token) => textLower === token || textLower.includes(token));
                            });
                        if (membersLink) return true;
                        const article = document.querySelector('div[role="feed"], div[data-pagelet]');
                        return !!(article && article.getBoundingClientRect().height > 200);
                    }""",
                    {"tokens": list(self.MEMBERS_TAB_LABELS)},
                )
            )
        except Exception:
            return False

    def _has_members_page_ready(self):
        try:
            snapshot = self._capture_members_page_ready_snapshot()
            if not snapshot:
                self._last_members_ready_stable_rounds = 0
                return False
            visible_count = int(snapshot.get("visibleCount") or 0)
            fingerprint = str(snapshot.get("fingerprint") or "").strip()
            page_stable = (
                bool(snapshot.get("looksLikeMembers"))
                or bool(snapshot.get("hasHeading"))
                or visible_count >= 2
            )
            list_ready = visible_count >= 1 and bool(fingerprint)
            self._set_last_scope_status(False, "scope_pending", "")
            self._set_last_member_list_snapshot(
                fingerprint=fingerprint,
                visible_count=visible_count,
                anchor_found=True,
                anchor_at_tail=False,
                scope_confirmed=False,
                scope_reason="scope_pending",
                scope_label="",
            )
            if not page_stable or not list_ready:
                self._last_members_ready_snapshot = snapshot
                self._last_members_ready_stable_rounds = 0
                return False
            previous = self._last_members_ready_snapshot or {}
            is_stable = self._members_ready_snapshot_stable(previous, snapshot)
            self._last_members_ready_snapshot = snapshot
            if is_stable:
                self._last_members_ready_stable_rounds += 1
            else:
                self._last_members_ready_stable_rounds = 0
            if bool(snapshot.get("looksLikeMembers")) and visible_count >= 1:
                return True
            if bool(snapshot.get("hasHeading")) and visible_count >= 2:
                return True
            return self._last_members_ready_stable_rounds >= 1
        except Exception:
            self._last_members_ready_stable_rounds = 0
            return False

    def _capture_members_page_ready_snapshot(self):
        try:
            return self.page.evaluate(
                """(payload) => {
                    const headings = payload.headings || [];
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                    const headingsLower = headings.map((token) => normalize(token).toLowerCase()).filter(Boolean);
                    const visible = (el) => {
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                        if (rect.bottom < 0 || rect.right < 0 || rect.top > window.innerHeight || rect.left > window.innerWidth) return false;
                        return true;
                    };
                    const url = location.pathname || '';
                    const looksLikeMembers = url.includes('/members');
                    const main = document.querySelector('main, [role="main"]');
                    const mainVisible = !!(main && visible(main));
                    const loadingIndicators = Array.from(
                        document.querySelectorAll('[role="progressbar"], [aria-busy="true"], [data-visualcompletion="loading-state"]')
                    ).filter((el) => visible(el)).length;
                    const hasHeading = Array.from(document.querySelectorAll('h1, h2, h3, span, div'))
                        .some((el) => {
                            if (!visible(el)) return false;
                            const text = normalize(el.innerText || '');
                            const textLower = text.toLowerCase();
                            return headingsLower.some((token) => textLower === token || textLower.includes(token));
                        });
                    const ids = [];
                    const seen = new Set();
                    const root = mainVisible ? main : document;
                    for (const link of Array.from(root.querySelectorAll('a[href*="/groups/"][href*="/user/"]'))) {
                        if (!visible(link)) continue;
                        const href = link.getAttribute('href') || '';
                        const match = href.match(/\\/groups\\/[^/]+\\/user\\/(\\d+)\\/?/i);
                        const userId = match && match[1] ? match[1] : href;
                        if (seen.has(userId)) continue;
                        seen.add(userId);
                        ids.push(userId);
                        if (ids.length >= 12) break;
                    }
                    const ready =
                        mainVisible &&
                        loadingIndicators === 0 &&
                        ids.length >= 1 &&
                        (looksLikeMembers || hasHeading);
                    return {
                        looksLikeMembers,
                        mainVisible,
                        loadingIndicators,
                        hasHeading,
                        visibleCount: ids.length,
                        fingerprint: ids.join('|'),
                        ready,
                    };
                }""",
                {"headings": list(self.MEMBERS_PAGE_HINTS)},
            )
        except Exception:
            return None

    def _members_ready_snapshot_stable(self, previous, current):
        if not previous or not current:
            return False
        return (
            bool(previous.get("mainVisible")) == bool(current.get("mainVisible"))
            and int(previous.get("loadingIndicators") or 0) == int(current.get("loadingIndicators") or 0)
            and
            bool(previous.get("looksLikeMembers")) == bool(current.get("looksLikeMembers"))
            and bool(previous.get("hasHeading")) == bool(current.get("hasHeading"))
            and int(previous.get("visibleCount") or 0) == int(current.get("visibleCount") or 0)
            and str(previous.get("fingerprint") or "") == str(current.get("fingerprint") or "")
            and bool(current.get("ready"))
        )

    def _wait_for_member_list_stable(self, timeout=1.6, stable_rounds=2, interval=0.12):
        timeout = max(0.3, float(timeout or 0.3))
        interval = max(0.05, float(interval or 0.05))
        end_time = time.time() + timeout
        previous = None
        stable = 0
        while time.time() < end_time:
            paused_seconds = self._wait_if_paused()
            if paused_seconds:
                end_time += paused_seconds
            snapshot = self._capture_members_page_ready_snapshot()
            if not snapshot:
                previous = None
                stable = 0
                self._pause_aware_sleep(interval)
                continue
            page_stable = bool(snapshot.get("ready"))
            self._set_last_scope_status(False, "scope_pending", "")
            self._set_last_member_list_snapshot(
                fingerprint=snapshot.get("fingerprint"),
                visible_count=snapshot.get("visibleCount"),
                anchor_found=True,
                anchor_at_tail=False,
                scope_confirmed=False,
                scope_reason="scope_pending",
                scope_label="",
            )
            if not page_stable:
                previous = snapshot
                stable = 0
                self._pause_aware_sleep(interval)
                continue
            if previous and self._members_ready_snapshot_stable(previous, snapshot):
                stable += 1
                if stable >= stable_rounds:
                    self._last_members_ready_snapshot = snapshot
                    self._last_members_ready_stable_rounds = stable
                    return snapshot
            else:
                stable = 0
            previous = snapshot
            self._pause_aware_sleep(interval)
        return None

    def _adjust_sequence_view_after_scroll(self, scope=None):
        hint = dict(self._sequence_anchor_hint or {})
        anchor_user_id = str(hint.get("anchor_user_id") or "").strip()
        if not hint.get("active") or not hint.get("anchor_at_tail") or not anchor_user_id:
            return False
        try:
            adjusted = self.page.evaluate(
                """(payload) => {
                    const anchorUserId = String(payload.anchorUserId || '').trim();
                    const scope = payload.scope || {};
                    if (!anchorUserId) return { adjusted: false };
                    const pageY = window.scrollY || 0;
                    const sectionTop = Number(scope.sectionTop || 0);
                    const headingBottom = Number(scope.headingBottom || sectionTop || 0);
                    const scopeBottom = Number(scope.sectionBottom || 0);
                    const scopeLeft = Number(scope.sectionLeft || 0);
                    const scopeRight = Number(scope.sectionRight || window.innerWidth);
                    const desiredTop = Math.max(80, headingBottom - pageY + 18);
                    const links = Array.from(document.querySelectorAll(`a[href*="/user/${anchorUserId}/"]`));
                    for (const link of links) {
                        const rect = link.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        const absTop = rect.top + pageY;
                        const absBottom = rect.bottom + pageY;
                        if (absTop < sectionTop - 12 || absBottom > scopeBottom + 40) continue;
                        if (rect.right < scopeLeft - 32 || rect.left > scopeRight + 96) continue;
                        const nextScrollY = Math.max(0, pageY + rect.top - desiredTop);
                        if (Math.abs(nextScrollY - pageY) <= 4) {
                            return { adjusted: false, anchorTop: rect.top };
                        }
                        window.scrollTo({ top: nextScrollY, behavior: 'auto' });
                        return { adjusted: true, anchorTop: rect.top, nextScrollY };
                    }
                    return { adjusted: false };
                }""",
                {
                    "anchorUserId": anchor_user_id,
                    "scope": scope or self._locate_newcomers_scope(),
                },
            )
        except Exception:
            return False
        if adjusted and adjusted.get("adjusted"):
            self._wait_for_scroll_stable(timeout=1.1, member_scope=scope)
            log.info(f"{self._prefix()}顺序续跑视图已调整：上一位成员靠近顶部，继续承接下一位。")
            return True
        return False

    def _scroll_joined_groups_container(self, to_top=False, step_ratio=0.85, min_px=900, smooth=False):
        try:
            return self.page.evaluate(
                """(params) => {
                    const { toTop, ratio, minPx, mode } = params;
                    const candidates = Array.from(document.querySelectorAll('div, section, ul'))
                        .map((el) => {
                            const style = window.getComputedStyle(el);
                            if (!style) return null;
                            const overflowY = style.overflowY || style.overflow;
                            if (!['auto', 'scroll'].includes(overflowY)) return null;
                            if (el.scrollHeight <= el.clientHeight + 5) return null;
                            const rect = el.getBoundingClientRect();
                            const links = el.querySelectorAll('a[href*="/groups/"]');
                            return {
                                el,
                                rect,
                                count: links.length,
                            };
                        })
                        .filter(Boolean)
                        .filter((item) => item.count > 0)
                        .filter((item) => item.rect.left >= 0 && item.rect.left < window.innerWidth * 0.6);

                    if (!candidates.length) return null;
                    candidates.sort((a, b) => b.count - a.count);
                    const target = candidates[0].el;
                    if (!target) return null;

                    if (toTop) {
                        target.scrollTop = 0;
                        return true;
                    }

                    const before = target.scrollTop;
                    const step = Math.max(target.clientHeight * ratio, minPx);
                    target.scrollBy({ top: step, behavior: mode });
                    return target.scrollTop > before + 2;
                }""",
                {
                    "toTop": bool(to_top),
                    "ratio": step_ratio,
                    "minPx": min_px,
                    "mode": "smooth" if smooth else "auto",
                    "labels": list(self.NEWCOMER_SECTION_LABELS),
                },
            )
        except Exception:
            return None

    def _extract_visible_member_candidates(self, anchor_user_id=None):
        if self.page.is_closed():
            return {
                "items": [],
                "anchorFound": False,
                "scopeConfirmed": False,
                "scopeReason": "page_closed",
                "scopeLabel": "",
            }
        scope = self._locate_newcomers_scope()
        if not scope.get("confirmed") and self._can_attempt_newcomers_scope_recovery(scope):
            recovered_scope = self._recover_newcomers_scope_from_last_confirmed(scope)
            if recovered_scope and recovered_scope.get("confirmed"):
                scope = recovered_scope
        if not scope.get("confirmed"):
            return {
                "items": [],
                "anchorFound": False if anchor_user_id else True,
                "scopeConfirmed": False,
                "scopeReason": str(scope.get("reason") or "newcomers_scope_unconfirmed").strip(),
                "scopeLabel": str(scope.get("label") or "").strip(),
            }
        for attempt in range(3):
            try:
                return self.page.evaluate(
                    """(payload) => {
                const anchorUserId = payload.anchorUserId || '';
                const scope = payload.scope || {};
                const scopeSelector = scope.containerSelector || '';
                const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
                const lower = (value) => normalize(value).toLowerCase();
                const recentJoinRe = /(刚加入|刚刚加入|加入时间[:：]\\s*\\S+|(约|大约|大概)?\\s*\\d+\\s*(秒|分钟|小时|天|周|月|个月|年)前(?:加入)?|\\d{4}年\\d{1,2}月\\d{1,2}日由.+添加|joined\\s+(just now|just|\\d+\\s*(second|minute|hour|day|week|month|year)s?\\s+ago|on\\s+\\w+)|added\\s+by|se\\s+unió|hace\\s+\\d+|añadido\\s+por|entrou|há\\s+\\d+|adicionado\\s+por|a\\s+rejoint|il\\s+y\\s+a|ajouté\\s+par|beigetreten|vor\\s+\\d+|si\\s+è\\s+unito|fa\\s+\\d+|aggiunto\\s+da|toegetreden|geleden|присоединился|назад|dołączył|temu|katıldı|önce|انضم|منذ|शामिल|पहले|bergabung|yang\\s+lalu|menyertai|yang\\s+lalu|tham\\s+gia|trước|参加|前|가입|전|เพิ่งเข้าร่วม|เข้าร่วมเมื่อ|เข้าร่วม|เมื่อ\\s*\\d+\\s*(วินาที|นาที|ชั่วโมง|วัน|สัปดาห์|เดือน|ปี)ที่แล้ว|ចូលរួម|ເຂົ້າຮ່ວມ|sumali|nakaraan)/i;
                const groupUserRe = /\\/groups\\/[^/]+\\/user\\/\\d+\\/?/i;
                const skipNames = new Set((payload.skipLabels || []).map(lower).filter(Boolean));
                const actionHintKeywords = (payload.actionHintKeywords || []).map(lower).filter(Boolean);
                const blockedRoleTexts = payload.blockedRoleTexts || [];
                const results = [];
                const headingBottom = Number(scope.headingBottom || scope.sectionTop || 0);
                const scopeTop = Number(scope.sectionTop || 0);
                const scopeBottom = Number(scope.sectionBottom || 0);
                const scopeLeft = Number(scope.sectionLeft || 0);
                const scopeRight = Number(scope.sectionRight || window.innerWidth);
                const sectionLabel = scope.label || '';
                if (!headingBottom || !scopeBottom || scopeBottom <= headingBottom + 20) {
                    return {
                        items: [],
                        anchorFound: anchorUserId ? false : true,
                        scopeConfirmed: false,
                        scopeReason: 'newcomers_scope_unconfirmed',
                        scopeLabel: sectionLabel,
                    };
                }
                const laneMinX = Math.max(0, scopeLeft - 32);
                const laneMaxX = Math.min(window.innerWidth, scopeRight + 96);
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
                    const hasBlockedRole = (text) => blockedRoleTexts.some((token) => text.includes(token));
                    const getVisibleGroupUserIds = (node) => {
                        if (!node || !node.querySelectorAll) return [];
                        const ids = new Set();
                        for (const link of Array.from(node.querySelectorAll('a[href]'))) {
                            const href = link.getAttribute('href') || '';
                            if (!groupUserRe.test(href)) continue;
                            const match = href.match(/\\/user\\/(\\d+)/i) || href.match(/[?&]id=(\\d+)/i);
                            if (!match || !match[1]) continue;
                            const linkRect = link.getBoundingClientRect();
                            if (!linkRect.width || !linkRect.height) continue;
                            ids.add(match[1]);
                        }
                        return Array.from(ids);
                    };
                    const scopeRoot = scopeSelector ? document.querySelector(scopeSelector) : null;
                    const linkNodes = Array.from(
                        (scopeRoot || document).querySelectorAll('a[href*="/groups/"][href*="/user/"]')
                    );

                    let order = 0;
                    for (const a of linkNodes) {
                    const href = a.getAttribute('href') || '';
                    if (!href || href.startsWith('#')) continue;
                    if (!groupUserRe.test(href)) continue;
                    const userMatch = href.match(/\\/user\\/(\\d+)/i) || href.match(/[?&]id=(\\d+)/i);
                    const userId = userMatch && userMatch[1] ? userMatch[1] : '';
                    if (!userId) continue;

                    const rect = a.getBoundingClientRect();
                    const absTop = rect.top + window.scrollY;
                    const absBottom = rect.bottom + window.scrollY;
                    if (!rect.width || !rect.height) continue;
                    if (rect.bottom < 0 || rect.top > window.innerHeight) continue;
                    if (absTop < headingBottom - 8 || absBottom > scopeBottom + 28) continue;
                    if (rect.right < laneMinX || rect.left > laneMaxX) continue;

                    const name = (a.innerText || a.getAttribute('aria-label') || '')
                        .trim()
                        .replace(/\\s+/g, ' ');
                    const nameLower = name.toLowerCase();
                    if (!name || name.length < 2 || name.length > 80) continue;
                    if (skipNames.has(nameLower)) continue;

                    let card = a;
                    let bestCard = null;
                    let bestScore = -1;
                    for (let depth = 0; depth < 7 && card; depth += 1) {
                        const cardRect = card.getBoundingClientRect();
                        const text = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                        const textLower = text.toLowerCase();
                        if (text && text.length <= 420 && text.includes(name)) {
                            const memberIds = getVisibleGroupUserIds(card);
                            let score = 2;
                            if (recentJoinRe.test(text)) score += 4;
                            if (actionHintKeywords.some((token) => textLower.includes(token))) score += 1;
                            if (card.getAttribute && card.getAttribute('role') === 'listitem') score += 2;
                            if (text.includes(name + ' ')) score += 1;
                            if (text.length >= 24) score += 1;
                            if (memberIds.length === 1) score += 3;
                            if (memberIds.length > 1) score -= Math.min(8, memberIds.length * 3);
                            const absCardTop = cardRect.top + window.scrollY;
                            const absCardBottom = cardRect.bottom + window.scrollY;
                            if (cardRect.width >= rect.width && cardRect.height >= rect.height) score += 1;
                            if (absCardTop >= scopeTop - 48 && absCardBottom <= scopeBottom + 64) score += 2;
                            if (cardRect.height > 260) score -= 3;
                            if (cardRect.height > 420) score -= 5;
                            if (cardRect.width > Math.max(760, window.innerWidth * 0.7)) score -= 2;
                            if (score > bestScore) {
                                bestScore = score;
                                bestCard = card;
                            }
                        }
                        card = card.parentElement;
                    }

                    card = bestCard;
                    if (!card) continue;
                    const cardRect = card.getBoundingClientRect();
                    const absCardTop = cardRect.top + window.scrollY;
                    const absCardBottom = cardRect.bottom + window.scrollY;
                    if (absCardTop < scopeTop - 8 || absCardBottom > scopeBottom + 40) continue;
                    const cardText = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                    const cardMemberIds = getVisibleGroupUserIds(card);
                    if (!cardText.includes(name)) continue;
                    if (cardMemberIds.length !== 1) continue;
                    let blockedLocally = false;
                    let roleProbe = card;
                    for (let depth = 0; depth < 3 && roleProbe; depth += 1) {
                        const probeRect = roleProbe.getBoundingClientRect();
                        const probeText = (roleProbe.innerText || '').trim().replace(/\\s+/g, ' ');
                        const probeMemberIds = getVisibleGroupUserIds(roleProbe);
                        if (probeMemberIds.length > 1) break;
                        if (
                            probeText
                            && probeText.includes(name)
                            && hasBlockedRole(probeText)
                            && probeRect.height <= 260
                            && probeRect.width <= Math.max(900, window.innerWidth * 0.82)
                        ) {
                            blockedLocally = true;
                            break;
                        }
                        roleProbe = roleProbe.parentElement;
                    }
                    if (blockedLocally || hasBlockedRole(cardText)) continue;

                    results.push({
                        href,
                        userId,
                        name,
                        selector: cssPath(a),
                        cardText: cardText.slice(0, 200),
                        top: rect.top,
                        left: rect.left,
                        absTop,
                        absLeft: rect.left + window.scrollX,
                        order: order++,
                        sourceScope: 'newcomers',
                        scopeLabel: sectionLabel,
                    });
                }

                    results.sort((a, b) => a.top - b.top || a.left - b.left || a.order - b.order);
                    const deduped = [];
                    const seenUserIds = new Set();
                    for (const item of results) {
                        const userId = String(item.userId || '').trim();
                        if (!userId) continue;
                        if (seenUserIds.has(userId)) continue;
                        seenUserIds.add(userId);
                        item.isTail = false;
                        deduped.push(item);
                    }
                    if (deduped.length > 0) {
                        deduped[deduped.length - 1].isTail = true;
                    }
                    const listFingerprint = deduped
                        .slice(0, 12)
                        .map((item) => String(item.userId || '').trim())
                        .filter(Boolean)
                        .join('|');
                    if (!anchorUserId) {
                        return {
                        items: deduped,
                        fallbackItems: [],
                        anchorFound: true,
                        anchorAtTail: false,
                        scopeConfirmed: true,
                        scopeReason: 'ok',
                        scopeLabel: sectionLabel,
                        listFingerprint,
                        visibleCount: deduped.length,
                    };
                }
                const anchorToken = String(anchorUserId);
                const anchorIndex = deduped.findIndex((item) => String(item.userId || '') === anchorToken);
                if (anchorIndex >= 0) {
                    const afterAnchor = deduped.filter((item, index) => index > anchorIndex && String(item.userId || '') !== anchorToken);
                    return {
                        items: afterAnchor,
                        fallbackItems: deduped,
                        anchorFound: true,
                        anchorAtTail: anchorIndex === deduped.length - 1,
                        scopeConfirmed: true,
                        scopeReason: 'ok',
                        scopeLabel: sectionLabel,
                        listFingerprint,
                        visibleCount: deduped.length,
                    };
                }
                    return {
                        items: [],
                        fallbackItems: deduped,
                        anchorFound: false,
                        anchorAtTail: false,
                        scopeConfirmed: true,
                        scopeReason: 'ok',
                        scopeLabel: sectionLabel,
                        listFingerprint,
                        visibleCount: deduped.length,
                    };
            }""",
                    {
                        "anchorUserId": anchor_user_id or "",
                        "scope": scope,
                        "blockedRoleTexts": list(self.NEWCOMER_BLOCKED_ROLE_TEXTS),
                        "skipLabels": list(self.MEMBER_ACTION_SKIP_LABELS),
                        "actionHintKeywords": list(self.MEMBER_ACTION_HINT_KEYWORDS),
                    },
                )
            except Exception as exc:
                message = str(exc)
                if "Execution context was destroyed" in message or "Target page, context or browser has been closed" in message:
                    if attempt == 0:
                        log.warning("成员列表页面发生跳转，重试抓取成员列表...")
                    self._pause_aware_sleep(0.2 + 0.2 * attempt)
                    continue
                raise
        return {
            "items": [],
            "anchorFound": False,
            "anchorAtTail": False,
            "scopeConfirmed": False,
            "scopeReason": "newcomers_scope_unconfirmed",
            "scopeLabel": "",
            "listFingerprint": "",
            "visibleCount": 0,
        }

    def _build_member_targets_from_items(self, items, current_user_url):
        targets = []
        seen_urls = set()
        for item in items or []:
            target = self._build_member_target(item)
            if not target:
                continue
            profile_url = target.get("profile_url") or ""
            if not profile_url or profile_url == current_user_url:
                continue
            if profile_url in seen_urls:
                continue
            if target.get("source_scope") != "newcomers":
                continue
            seen_urls.add(profile_url)
            targets.append(target)
        return targets

    def _normalize_user_url(self, href):
        full_url = urljoin("https://www.facebook.com", href)
        parsed = urlparse(full_url)
        if "facebook.com" not in parsed.netloc:
            return None

        path = parsed.path.rstrip("/")
        if path == "/profile.php":
            user_id = parse_qs(parsed.query).get("id", [None])[0]
            if not user_id:
                return None
            return f"https://www.facebook.com/profile.php?id={user_id}"

        segments = [segment for segment in path.split("/") if segment]
        if len(segments) >= 4 and segments[0] == "groups" and segments[2] == "user":
            return f"https://www.facebook.com/profile.php?id={segments[3]}"
        if len(segments) == 2 and segments[0] == "user":
            return f"https://www.facebook.com/profile.php?id={segments[1]}"
        if len(segments) != 1:
            return None

        top_level = segments[0].lower()
        if top_level in self.RESERVED_TOP_LEVEL_PATHS:
            return None
        return f"https://www.facebook.com/{segments[0]}"

    def _build_member_target(self, item):
        href = item.get("href") or ""
        full_url = urljoin("https://www.facebook.com", href)
        parsed = urlparse(full_url)
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) < 4 or segments[0] != "groups" or segments[2] != "user":
            return None

        user_id = segments[3]
        profile_url = self._normalize_user_url(full_url)
        if not profile_url:
            return None

        return {
            "name": item.get("name", "").strip(),
            "user_id": user_id,
            "group_user_url": f"https://www.facebook.com/groups/{segments[1]}/user/{user_id}/",
            "profile_url": profile_url,
            "link_selector": item.get("selector") or "",
            "card_text": item.get("cardText", ""),
            "abs_top": item.get("absTop"),
            "abs_left": item.get("absLeft"),
            "is_tail": bool(item.get("isTail")),
            "source_scope": item.get("sourceScope") or "",
            "scope_label": item.get("scopeLabel") or "",
        }

    def _extract_group_root_url(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url))
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) >= 2 and segments[0] == "groups":
            return f"https://www.facebook.com/groups/{segments[1]}"
        return None

    def _extract_group_id(self, url):
        group_root = self._extract_group_root_url(url)
        if not group_root:
            return None
        parsed = urlparse(group_root)
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) >= 2 and segments[0] == "groups":
            return segments[1].strip() or None
        return None

    def _is_group_root_url(self, url, expected_root=None):
        group_root = self._extract_group_root_url(url)
        if not group_root:
            return False
        parsed = urlparse(urljoin("https://www.facebook.com", url))
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) != 2 or segments[0] != "groups":
            return False
        if expected_root and group_root != expected_root:
            return False
        return True

    def _is_group_members_page(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url))
        return parsed.path.startswith("/groups/") and "/members" in parsed.path

    def _get_current_user_url(self):
        if self._current_user_id is None:
            try:
                cookies = self.page.context.cookies("https://www.facebook.com")
            except Exception:
                cookies = []
            self._current_user_id = next(
                (cookie.get("value") for cookie in cookies if cookie.get("name") == "c_user"),
                "",
            )

        if not self._current_user_id:
            return None
        return f"https://www.facebook.com/profile.php?id={self._current_user_id}"
