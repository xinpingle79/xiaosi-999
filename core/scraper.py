import random
import time
from urllib.parse import parse_qs, urljoin, urlparse

from utils.logger import log


class Scraper:
    NEWCOMER_SECTION_LABELS = (
        "小组新人",
        "最近加入",
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
    JOINED_GROUP_SKIP_LABELS = {
        "查看全部",
        "See all",
        "View all",
        "Ver todo",
        "Ver todos",
        "Voir tout",
        "Alle anzeigen",
        "Mostra tutti",
        "Alles bekijken",
        "ดูทั้งหมด",
        "新建小组",
        "Create new group",
        "你的小组",
        "Your groups",
        "发现小组",
        "Discover groups",
    }
    MEMBERS_PAGE_HINTS = (
        "Members",
        "People",
        "成员",
        "成员页",
        "成员列表",
        "成员资料",
        "สมาชิก",
        "สมาชิกในกลุ่ม",
        "ผู้คน",
        "ผู้คนในกลุ่ม",
        "People you may know",
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

    def __init__(self, page, window_label=None):
        self.page = page
        self._current_user_id = None
        self.window_label = (window_label or "").strip()
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

    def _locate_newcomers_scope(self):
        try:
            scope = self.page.evaluate(
                """(payload) => {
                    const labels = payload.labels || [];
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
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
                            if (el.offsetParent === null) continue;
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
                                    if (link.offsetParent === null) return false;
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
                            label: '',
                        };
                    }
                    return best;
                }""",
                {"labels": list(self.NEWCOMER_SECTION_LABELS)},
            )
            self._last_scope_snapshot = dict(scope or {}) if isinstance(scope, dict) else None
            return scope
        except Exception as exc:
            self._last_scope_snapshot = None
            return {
                "confirmed": False,
                "reason": f"newcomers_section_lookup_failed:{exc}",
                "label": "",
            }

    def _joined_groups_snapshot(self):
        try:
            return self.page.evaluate(
                """(payload) => {
                    const labels = payload.labels || [];
                    const blacklist = payload.blacklist || [];
                    const exactSkips = new Set(payload.skipLabels || []);
                    const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
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
                    const isValidGroupLink = (href) => {
                        try {
                            const url = new URL(href, location.origin);
                            const parts = url.pathname.split('/').filter(Boolean);
                            return parts.length === 2 && parts[0] === 'groups' && !blacklist.includes(parts[1]);
                        } catch {
                            return false;
                        }
                    };

                    const collectFromContainer = (container) => {
                        const items = [];
                        const seen = new Set();
                        for (const a of Array.from(container.querySelectorAll('a[href*="/groups/"]'))) {
                            if (a.offsetParent === null) continue;
                            const href = a.getAttribute('href') || '';
                            if (!isValidGroupLink(href)) continue;
                            const rect = a.getBoundingClientRect();
                            if (!rect.width || !rect.height) continue;
                            const text = normalize(a.innerText || (a.getAttribute && a.getAttribute('aria-label')));
                            if (!text || text.length < 2 || exactSkips.has(text)) continue;
                            const full = new URL(href, location.origin).href;
                            if (seen.has(full)) continue;
                            seen.add(full);
                            items.push({ href: full, text, top: rect.top, left: rect.left });
                        }
                        items.sort((a, b) => a.top - b.top || a.left - b.left);
                        return items;
                    };

                    const labelNodes = Array.from(
                        document.querySelectorAll('h1, h2, h3, h4, [role="heading"], [aria-level], div, span')
                    );
                    for (const el of labelNodes) {
                        if (el.offsetParent === null) continue;
                        const text = normalize(el.innerText);
                        const matched = labels.find((label) => matchesLabel(text, label));
                        if (!matched) continue;
                        if (text.length > matched.length + 32) continue;
                        let node = el;
                        const candidates = [];
                        for (let depth = 0; depth < 7 && node; depth += 1) {
                            const items = collectFromContainer(node);
                            if (items.length) {
                                const rect = node.getBoundingClientRect();
                                if (rect.width >= 220 && rect.height >= 120) {
                                    candidates.push({ rect, items });
                                }
                            }
                            node = node.parentElement;
                        }
                        if (candidates.length) {
                            candidates.sort((a, b) => (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height));
                            return {
                                ready: true,
                                source: 'labeled_section',
                                items: candidates[0].items,
                            };
                        }
                    }

                    const path = location.pathname || '';
                    const onGroupsSurface = path === '/groups' || path === '/groups/' || path.startsWith('/groups/feed') || path.startsWith('/groups/joins');

                    const containerCandidates = Array.from(document.querySelectorAll('div, section, ul, aside'))
                        .map((el) => {
                            if (el.offsetParent === null) return null;
                            const rect = el.getBoundingClientRect();
                            if (!rect.width || !rect.height) return null;
                            if (rect.left > window.innerWidth * 0.72) return null;
                            if (rect.top > window.innerHeight * 0.82) return null;
                            const items = collectFromContainer(el);
                            if (items.length < 3) return null;
                            return { rect, items };
                        })
                        .filter(Boolean);
                    if (!containerCandidates.length || !onGroupsSurface) {
                        return { ready: false, source: 'not_found', items: [] };
                    }
                    containerCandidates.sort((a, b) => {
                        const countDiff = b.items.length - a.items.length;
                        if (countDiff !== 0) return countDiff;
                        return (a.rect.width * a.rect.height) - (b.rect.width * b.rect.height);
                    });
                    return {
                        ready: true,
                        source: 'container_fallback',
                        items: containerCandidates[0].items,
                    };
                }""",
                {
                    "labels": list(self.JOINED_GROUP_SECTION_LABELS),
                    "skipLabels": list(self.JOINED_GROUP_SKIP_LABELS),
                    "blacklist": list(self.GROUP_ROUTE_BLACKLIST),
                },
            )
        except Exception:
            return {"ready": False, "source": "error", "items": []}

    def _wait_for_condition(self, step_name, detail, checker, timeout=12.0, interval=0.35):
        timeout = max(0.5, float(timeout or 0.5))
        interval = max(0.05, float(interval or 0.05))
        start = time.time()
        log.info(f"{self._prefix()}{step_name}，{detail}...")
        last_error = None
        while time.time() - start < timeout:
            try:
                if checker():
                    waited = time.time() - start
                    log.info(f"{self._prefix()}{step_name}等待成功，耗时 {waited:.1f} 秒。")
                    return True
            except Exception as exc:
                last_error = exc
            time.sleep(interval)
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

    def open_groups_feed(self):
        if not self._adopt_existing_facebook_page():
            log.warning(f"{self._prefix()}未找到可复用的 Facebook 页面，无法按手动路径进入小组。")
            return False
        if not self._is_groups_surface(self.page.url):
            if not self._open_home_surface_for_groups_entry():
                log.warning(f"{self._prefix()}未能回到 Facebook 首页，无法按手动路径进入小组。")
                return False
            click_info = self._click_groups_entry()
            if not click_info:
                log.warning(f"{self._prefix()}未找到“小组”入口，无法进入小组页面。")
                return False
            self._wait_for_groups_list_ready(timeout=12.0, step_name="小组首页")
        return self._has_groups_list_ready()

    def collect_joined_group_urls(self, max_scrolls=8):
        if not self.open_groups_feed():
            return []
        if not self._open_joined_groups_listing():
            return []
        self._wait_for_groups_list_ready(timeout=6.0, step_name="已加入小组列表")
        self._scroll_joined_groups_container(to_top=True)

        group_urls = []
        seen_urls = set()
        idle_rounds = 0
        round_idx = 0
        unlimited = not max_scrolls or int(max_scrolls) <= 0

        while unlimited or round_idx < int(max_scrolls):
            batch = self._extract_joined_group_urls()
            if not batch:
                self._wait_for_groups_list_ready(timeout=3.0, step_name="已加入小组列表")
                batch = self._extract_joined_group_urls()
            added = 0
            for item in batch:
                group_root = self._extract_group_root_url(item.get("href"))
                if not group_root or group_root in seen_urls:
                    continue
                seen_urls.add(group_root)
                group_urls.append(group_root)
                added += 1

            log.info(
                f"正在收集已加入小组 (轮次: {round_idx + 1}{'' if unlimited else '/' + str(max_scrolls)})，"
                f"本轮新增 {added} 个，累计 {len(group_urls)} 个"
            )

            idle_rounds = idle_rounds + 1 if added == 0 else 0
            moved = self._scroll_joined_groups_container()
            if moved is None:
                moved = self.scroll_page()
            if not moved and idle_rounds >= 2:
                break
            if idle_rounds >= 5:
                break
            round_idx += 1

        return group_urls

    def open_group_members_page(self, group_url):
        group_root = self._extract_group_root_url(group_url)
        if not group_root:
            log.error(f"❌ 无法识别目标小组链接: {group_url}")
            return False

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
        if not self._open_members_tab():
            log.warning(f"{self._prefix()}未能通过点击进入成员页，跳过当前小组: {group_url}")
            return False

        members_ready = self._wait_for_members_page_ready(timeout=24.0)
        if not members_ready:
            log.warning(f"{self._prefix()}成员列表未完成加载，跳过当前小组: {group_url}")
            return False
        if not self.is_group_members_page():
            log.warning(f"{self._prefix()}当前 URL 未明确显示 members，但成员列表已可用: {self.page.url}")

        log.success(f"{self._prefix()}✅ 已进入成员视图: {self.page.url}")
        return True

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
        for round_index in range(max_rounds):
            scope = self._locate_newcomers_scope()
            if scope.get("confirmed"):
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
            if not moved:
                break
            stable = self._wait_for_member_list_stable(timeout=1.4)
            if stable:
                log.info(f"{self._prefix()}下滑后成员列表已稳定，继续查找“小组新人”。")

        log.warning(f"{self._prefix()}未找到“小组新人”标题，跳过当前小组。")
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
                time.sleep(interval)
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
            time.sleep(interval)
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
            if self._is_joined_groups_surface(self.page.url) or self._has_groups_list_ready():
                self._wait_for_groups_list_ready(timeout=6.0, step_name="已加入小组列表")
                return True
            for _ in range(3):
                clicked = self.page.evaluate(
                    """(payload) => {
                    const joinedLabels = new Set(payload.joinedLabels || []);
                    const seeAllLabels = new Set(payload.seeAllLabels || []);
                    const sidebarMaxX = Math.min(window.innerWidth * 0.42, 520);
                    const normalize = (value) => (value || '').trim().replace(/\\s+/g, ' ');
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
                    let fallback = null;
                    for (const el of Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]'))) {
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || '');
                        if (!text) continue;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.right < 0 || rect.bottom < 0 || rect.top > window.innerHeight) continue;
                        if (rect.left > sidebarMaxX) continue;
                        const href = el.getAttribute('href') || '';
                        if (joinedLabels.has(text) && /\\/groups\\/(joins|feed)(\\/|\\?|$)?/i.test(href || '')) {
                            el.click();
                            return {
                                clickedText: text,
                                clickedSelector: cssPath(el),
                            };
                        }
                        if (seeAllLabels.has(text) && !fallback) {
                            fallback = {
                                el,
                                clickedText: text,
                                clickedSelector: cssPath(el),
                            };
                        }
                    }
                    if (fallback) {
                        fallback.el.click();
                        return {
                            clickedText: fallback.clickedText,
                            clickedSelector: fallback.clickedSelector,
                        };
                    }
                    return null;
                }"""
                    ,
                    {
                        "joinedLabels": list(self.JOINED_GROUP_SECTION_LABELS),
                        "seeAllLabels": ["查看全部", "See all"],
                    },
                )
                if clicked:
                    time.sleep(random.uniform(0.15, 0.35))
                    entered = self._wait_for_condition(
                        "已加入小组列表",
                        "等待进入“你的小组”页面",
                        lambda: self._is_joined_groups_surface(self.page.url) or self._has_groups_list_ready(),
                        timeout=10.0,
                        interval=0.3,
                    )
                    if entered:
                        self._wait_for_groups_list_ready(timeout=6.0, step_name="已加入小组列表")
                        return True
                time.sleep(0.35)
        except Exception:
            return False
        return self._is_joined_groups_surface(self.page.url) or self._has_groups_list_ready()

    def _ensure_facebook_home_surface(self):
        self._adopt_existing_facebook_page()

    def _adopt_existing_facebook_page(self):
        try:
            deadline = time.time() + 12.0
            while time.time() < deadline:
                if self._is_reusable_facebook_url(self.page.url) and self._has_real_facebook_surface(self.page):
                    try:
                        self.page.bring_to_front()
                    except Exception:
                        pass
                    return True
                for candidate in self.page.context.pages:
                    if candidate.is_closed():
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
                    log.info(f"{self._prefix()}已接管现成 Facebook 页面: {self.page.url}")
                    return True
                time.sleep(0.4)
            log.info(f"{self._prefix()}未发现现成 Facebook 页面，先进入 Facebook 首页。")
            self.page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
            try:
                self.page.wait_for_load_state("domcontentloaded", timeout=3000)
            except Exception:
                pass
            self._wait_for_condition(
                "Facebook首页",
                "等待 Facebook 首页真实内容出现",
                lambda: self._is_reusable_facebook_url(self.page.url) and self._has_real_facebook_surface(self.page),
                timeout=12.0,
                interval=0.4,
            )
            return self._is_reusable_facebook_url(self.page.url) and self._has_real_facebook_surface(self.page)
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

    def _has_real_facebook_surface(self, page=None):
        target_page = page or self.page
        try:
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
        if self._has_groups_sidebar_entry():
            return True
        if self._is_home_surface(self.page.url):
            self._wait_for_condition(
                "Facebook首页",
                "等待左侧“小组”入口出现",
                self._has_groups_sidebar_entry,
                timeout=10.0,
                interval=0.3,
            )
            return self._has_groups_sidebar_entry()
        clicked = self._click_home_entry()
        if not clicked:
            return False
        self._wait_for_condition(
            "Facebook首页",
            "等待左侧“小组”入口出现",
            self._has_groups_sidebar_entry,
            timeout=12.0,
            interval=0.3,
        )
        return self._has_groups_sidebar_entry()

    def _is_home_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = (parsed.path or "").rstrip("/")
        return path == ""

    def _has_groups_sidebar_entry(self):
        try:
            return bool(
                self.page.evaluate(
                    """(tokens) => {
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
                        return Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]')).some((el) => {
                            if (!visible(el)) return false;
                            const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                            if (!tokens.some((token) => text === token || text.includes(token))) return false;
                            const rect = el.getBoundingClientRect();
                            return rect.left < Math.min(window.innerWidth * 0.38, 420) && rect.top >= 72;
                        });
                    }""",
                    ["小组", "群组", "Groups", "Groupes", "Grupos", "กลุ่ม"],
                )
            )
        except Exception:
            return False

    def _click_home_entry(self):
        try:
            log.info(f"{self._prefix()}正在点击 Facebook 首页入口。")
            return self.page.evaluate(
                """(tokens) => {
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
                    let best = null;
                    for (const el of Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]'))) {
                        if (!visible(el)) continue;
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                        const href = el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        if (!tokens.some((token) => text === token || text.includes(token))) continue;
                        if (rect.left >= Math.min(window.innerWidth * 0.38, 420)) continue;
                        if (rect.top < 72) continue;
                        let score = 0;
                        if (/\\/groups\\/feed/i.test(href)) score += 240;
                        if (/\\/groups\\/?(\\?|$)/i.test(href)) score += 200;
                        if (/\\/groups\\/joins/i.test(href)) score += 180;
                        if (/\\/groups\\/(discover|create)/i.test(href) || /[?&]category=create/i.test(href)) score -= 240;
                        if (/\\/groups\\/[0-9A-Za-z_-]+/i.test(href)) score -= 160;
                        if (/\\/members/i.test(href)) score -= 200;
                        score += 120;
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
                ["小组", "群组", "Groups", "Groupes", "Grupos", "กลุ่ม"],
            )
        except Exception:
            return None

    def _open_target_group_page(self, group_root):
        try:
            log.info(f"{self._prefix()}正在点击目标小组: {group_root}")
            self._scroll_joined_groups_container(to_top=True)
            last_fingerprint = None
            idle_rounds = 0
            for _ in range(10):
                before_url = self.page.url
                clicked = self.page.evaluate(
                    """(targetGroupRoot) => {
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
                    group_root,
                )
                if clicked:
                    log.info(
                        f"{self._prefix()}目标小组候选命中: "
                        f"url={clicked.get('clickedUrl') or ''} "
                        f"type={clicked.get('clickedType') or ''} "
                        f"text={clicked.get('clickedText') or ''}"
                    )
                    self._wait_for_url_change(before_url, timeout=15.0)
                    current_url = str(self.page.url or "").strip()
                    if self._is_group_root_url(current_url, expected_root=group_root):
                        return True
                    if current_url.startswith(group_root + "/user/"):
                        log.warning(f"{self._prefix()}目标小组点击误入成员 profile，当前 URL: {current_url}")
                    elif current_url.startswith(group_root + "/"):
                        log.warning(f"{self._prefix()}目标小组点击误入子页面，当前 URL: {current_url}")
                    else:
                        log.warning(f"{self._prefix()}目标小组点击后未进入 group root，当前 URL: {current_url}")
                    try:
                        self.page.go_back(wait_until="domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    self._wait_for_groups_list_ready(timeout=6.0, step_name="已加入小组列表")
                snapshot = self._joined_groups_snapshot()
                fingerprint = "|".join(item.get("href") or "" for item in (snapshot.get("items") or [])[:12])
                idle_rounds = idle_rounds + 1 if fingerprint == last_fingerprint else 0
                moved = self._scroll_joined_groups_container()
                if moved is None:
                    moved = self.scroll_page(step_ratio=0.6, min_px=420, smooth=False)
                if not moved and idle_rounds >= 1:
                    break
                last_fingerprint = fingerprint
                time.sleep(0.25)
            return False
        except Exception:
            return False

    def _open_members_tab(self):
        try:
            log.info(f"{self._prefix()}正在点击“用户 / Members / 成员”tab。")
            before_url = self.page.url
            for _ in range(4):
                clicked = self.page.evaluate(
                    """(tokens) => {
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
                    for (const el of Array.from(document.querySelectorAll('a[href], [role="tab"], div[role="link"], span[role="link"]'))) {
                        if (!visible(el)) continue;
                        const text = normalize(el.innerText || el.getAttribute('aria-label') || el.getAttribute('title') || '');
                        const href = el.getAttribute('href') || '';
                        const rect = el.getBoundingClientRect();
                        const hasMembersHref = /\\/members(\\/|\\?|$)/i.test(href);
                        const hasMembersText = tokens.some((token) => text === token || text.includes(token));
                        if (!hasMembersHref && !hasMembersText) continue;
                        let score = 0;
                        if (hasMembersHref) score += 220;
                        if (hasMembersText) score += 180;
                        if (rect.y >= 210 && rect.y <= 340) score += 80;
                        if (rect.y < 190) score -= 120;
                        if ((text.includes('万位成员') || text.includes('查看所有人')) && rect.y < 220) score -= 160;
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
                    ["成员", "Members", "People", "成员列表", "成员资料", "用户"],
                )
                if clicked:
                    self._wait_for_url_change(before_url, timeout=15.0)
                    return True
                time.sleep(0.5)
            return False
        except Exception:
            return False

    def _wait_for_url_change(self, previous_url, timeout=15.0, interval=0.2):
        previous = str(previous_url or "").strip()
        end_time = time.time() + max(0.5, float(timeout or 0.5))
        last_url = self.page.url
        while time.time() < end_time:
            current_url = self.page.url
            if current_url != previous:
                return current_url
            last_url = current_url
            time.sleep(interval)
        return last_url

    def _is_groups_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = parsed.path or ""
        return path in {"/groups", "/groups/"} or path.startswith("/groups/feed") or path.startswith("/groups/joins")

    def _is_joined_groups_surface(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url or ""))
        path = parsed.path or ""
        return path.startswith("/groups/joins")

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
            return bool(snapshot.get("ready")) and bool(snapshot.get("items"))
        except Exception:
            return False

    def _has_group_page_ready(self):
        try:
            return bool(
                self.page.evaluate(
                    """() => {
                        const path = location.pathname || '';
                        if (!path.startsWith('/groups/') || path.includes('/members')) return false;
                        const main = document.querySelector('[role="main"]');
                        if (main && main.getBoundingClientRect().height > 200) return true;
                        const membersLink = Array.from(document.querySelectorAll('a[href], [role="tab"], div[role="link"], span[role="link"]'))
                            .find((el) => {
                                const href = el.getAttribute && (el.getAttribute('href') || '');
                                const text = (el.innerText || el.getAttribute('aria-label') || '').trim();
                                const rect = el.getBoundingClientRect();
                                if (!rect.width || !rect.height) return false;
                                if (rect.bottom < 0 || rect.top > window.innerHeight) return false;
                                if (/\\/members(\\/|\\?|$)/.test(href)) return true;
                                return ['成员', 'Members', 'People', '用户'].some((token) => text === token || text.includes(token));
                            });
                        if (membersLink) return true;
                        const article = document.querySelector('div[role="feed"], div[data-pagelet]');
                        return !!(article && article.getBoundingClientRect().height > 200);
                    }"""
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
            page_stable = bool(snapshot.get("looksLikeMembers")) and bool(snapshot.get("hasHeading"))
            list_ready = int(snapshot.get("visibleCount") or 0) >= 3 and bool(snapshot.get("fingerprint"))
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
                    const url = location.pathname || '';
                    const looksLikeMembers = url.includes('/members');
                    const hasHeading = Array.from(document.querySelectorAll('h1, h2, h3, span, div'))
                        .some((el) => {
                            const text = normalize(el.innerText || '');
                            return headings.some((token) => text === token || text.includes(token));
                        });
                    const ids = [];
                    const seen = new Set();
                    for (const link of Array.from(document.querySelectorAll('a[href*="/groups/"][href*="/user/"]'))) {
                        const rect = link.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.bottom < 0 || rect.top > window.innerHeight) continue;
                        const href = link.getAttribute('href') || '';
                        const match = href.match(/\\/groups\\/[^/]+\\/user\\/(\\d+)\\/?/i);
                        const userId = match && match[1] ? match[1] : href;
                        if (seen.has(userId)) continue;
                        seen.add(userId);
                        ids.push(userId);
                        if (ids.length >= 12) break;
                    }
                    return {
                        looksLikeMembers,
                        hasHeading,
                        visibleCount: ids.length,
                        fingerprint: ids.join('|'),
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
            bool(previous.get("looksLikeMembers")) == bool(current.get("looksLikeMembers"))
            and bool(previous.get("hasHeading")) == bool(current.get("hasHeading"))
            and int(previous.get("visibleCount") or 0) == int(current.get("visibleCount") or 0)
            and str(previous.get("fingerprint") or "") == str(current.get("fingerprint") or "")
            and int(current.get("visibleCount") or 0) >= 3
        )

    def _wait_for_member_list_stable(self, timeout=1.6, stable_rounds=2, interval=0.12):
        timeout = max(0.3, float(timeout or 0.3))
        interval = max(0.05, float(interval or 0.05))
        end_time = time.time() + timeout
        previous = None
        stable = 0
        while time.time() < end_time:
            snapshot = self._capture_members_page_ready_snapshot()
            if not snapshot:
                previous = None
                stable = 0
                time.sleep(interval)
                continue
            page_stable = bool(snapshot.get("looksLikeMembers")) and bool(snapshot.get("hasHeading"))
            list_stable = int(snapshot.get("visibleCount") or 0) >= 3 and bool(snapshot.get("fingerprint"))
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
            if not page_stable or not list_stable:
                previous = snapshot
                stable = 0
                time.sleep(interval)
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
            time.sleep(interval)
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
                const recentJoinRe = /(刚加入|刚刚加入|加入时间[:：]\\s*\\S+|(约|大约|大概)?\\s*\\d+\\s*(秒|分钟|小时|天|周|月|个月|年)前(?:加入)?|\\d{4}年\\d{1,2}月\\d{1,2}日由.+添加|joined\\s+(just now|just|\\d+\\s*(second|minute|hour|day|week|month|year)s?\\s+ago|on\\s+\\w+)|added\\s+by|se\\s+unió|hace\\s+\\d+|añadido\\s+por|entrou|há\\s+\\d+|adicionado\\s+por|a\\s+rejoint|il\\s+y\\s+a|ajouté\\s+par|beigetreten|vor\\s+\\d+|si\\s+è\\s+unito|fa\\s+\\d+|aggiunto\\s+da|toegetreden|geleden|присоединился|назад|dołączył|temu|katıldı|önce|انضم|منذ|शामिल|पहले|bergabung|yang\\s+lalu|menyertai|yang\\s+lalu|tham\\s+gia|trước|参加|前|가입|전|เพิ่งเข้าร่วม|เข้าร่วมเมื่อ|เข้าร่วม|เมื่อ\\s*\\d+\\s*(วินาที|นาที|ชั่วโมง|วัน|สัปดาห์|เดือน|ปี)ที่แล้ว|ចូលរួម|ເຂົ້າຮ່ວມ|sumali|nakaraan)/i;
                const groupUserRe = /\\/groups\\/[^/]+\\/user\\/\\d+\\/?/i;
                const skipNames = new Set(['查看全部', 'See all', '邀请', 'Invite', '发消息', 'Message', '更多', 'More']);
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
                const actionHintRe = /(发消息|发送消息|發送訊息|message|send message|add friend|follow|connect)/i;
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
                    if (!name || name.length < 2 || name.length > 80) continue;
                    if (skipNames.has(name)) continue;

                    let card = a;
                    let bestCard = null;
                    let bestScore = -1;
                    for (let depth = 0; depth < 7 && card; depth += 1) {
                        const cardRect = card.getBoundingClientRect();
                        const text = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                        if (text && text.length <= 420 && text.includes(name)) {
                            const memberIds = getVisibleGroupUserIds(card);
                            let score = 2;
                            if (recentJoinRe.test(text)) score += 4;
                            if (actionHintRe.test(text)) score += 1;
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
                    if (cardText.includes('管理员和版主') || cardText.includes('Admins and moderators')) continue;

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
                    },
                )
            except Exception as exc:
                message = str(exc)
                if "Execution context was destroyed" in message or "Target page, context or browser has been closed" in message:
                    if attempt == 0:
                        log.warning("成员列表页面发生跳转，重试抓取成员列表...")
                    time.sleep(0.2 + 0.2 * attempt)
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
