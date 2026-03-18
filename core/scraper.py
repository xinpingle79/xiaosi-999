import random
import time
from urllib.parse import parse_qs, urljoin, urlparse

from utils.logger import log


class Scraper:
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

    def __init__(self, page):
        self.page = page
        self._current_user_id = None

    def open_groups_feed(self):
        self._goto("https://www.facebook.com/groups/feed/", "小组首页")

    def collect_joined_group_urls(self, max_scrolls=8):
        self.open_groups_feed()
        self._open_joined_groups_listing()
        self._wait_for_groups_list_ready(timeout=6.0)
        self._scroll_joined_groups_container(to_top=True)

        group_urls = []
        seen_urls = set()
        idle_rounds = 0
        round_idx = 0
        unlimited = not max_scrolls or int(max_scrolls) <= 0

        while unlimited or round_idx < int(max_scrolls):
            batch = self._extract_joined_group_urls()
            if not batch:
                self._wait_for_groups_list_ready(timeout=3.0)
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

        members_url = f"{group_root}/members"
        self._goto(members_url, "小组成员页")

        if not self.is_group_members_page():
            log.warning(f"成员页直达未命中，回退到小组主页再尝试: {group_root}")
            self._goto(group_root, "目标小组主页", settle_delay_range=(0.8, 1.2))
            self._goto(members_url, "小组成员页", settle_delay_range=(1.0, 1.4))

        if not self.is_group_members_page():
            log.warning(f"当前 URL 未明确显示 members，但继续按成员页规则尝试: {self.page.url}")

        log.success(f"✅ 已进入成员视图: {self.page.url}")
        return True

    def focus_newcomers_section(self, max_scroll_rounds=40):
        labels = (
            "小组新人",
            "最近加入",
            "New to the group",
            "New members",
            "Recently joined",
            "New member",
            "Recently added",
            "สมาชิกใหม่",
            "สมาชิกใหม่ในกลุ่ม",
            "เพิ่งเข้าร่วม",
            "เข้าร่วมเมื่อไม่นานมานี้",
            "Thành viên mới",
            "Mới tham gia",
            "Vừa tham gia",
            "Anggota baru",
            "Baru bergabung",
            "Bergabung baru-baru ini",
            "Ahli baharu",
            "Baru menyertai",
            "Menyertai baru-baru ini",
            "Nuevos miembros",
            "Nuevos en el grupo",
            "Recién incorporados",
            "Novos membros",
            "Novo no grupo",
            "Ingressou recentemente",
            "Nouveaux membres",
            "Nouveau dans le groupe",
            "A rejoint récemment",
            "Neue Mitglieder",
            "Neu in der Gruppe",
            "Kürzlich beigetreten",
            "Nuovi membri",
            "Nuovo nel gruppo",
            "Iscritto di recente",
            "Nieuwe leden",
            "Nieuw in de groep",
            "Recent toegetreden",
            "Новые участники",
            "Новый участник",
            "Недавно присоединился",
            "Nowi członkowie",
            "Nowy w grupie",
            "Dołączył niedawno",
            "Yeni üyeler",
            "Yeni üye",
            "Yeni katıldı",
            "أعضاء جدد",
            "عضو جديد",
            "انضم حديثًا",
            "नए सदस्य",
            "नया सदस्य",
            "हाल ही में शामिल हुए",
            "新しいメンバー",
            "新メンバー",
            "最近参加",
            "새 멤버",
            "새 회원",
            "최근 가입",
            "សមាជិកថ្មី",
            "ສະມາຊິກໃໝ່",
            "Mga bagong miyembro",
            "Bagong miyembro",
            "Kamakailan lang sumali",
        )
        try:
            max_rounds = max(1, int(max_scroll_rounds or 1))
        except Exception:
            max_rounds = 40

        self.scroll_to_top()
        for _ in range(max_rounds):
            for label in labels:
                try:
                    heading = self.page.get_by_text(label, exact=True)
                    if heading.count() == 0:
                        heading = self.page.get_by_text(label, exact=False)
                    if heading.count() == 0:
                        continue
                    heading.first.scroll_into_view_if_needed(timeout=3000)
                    time.sleep(0.2)
                    self.page.evaluate("window.scrollBy(0, 220)")
                    time.sleep(0.4)
                    log.info(f"已定位到 {label} 区块，从这里开始采集。")
                    return True
                except Exception:
                    continue
            moved = self.scroll_member_list()
            if not moved:
                break
            time.sleep(0.2)

        log.warning("未找到“小组新人”标题，跳过当前小组。")
        return False

    def collect_visible_new_member_targets(self, anchor=None, require_after_anchor=False):
        current_user_url = self._get_current_user_url()
        targets = []
        seen_urls = set()
        anchor_user_id = anchor.get("user_id") if anchor else None

        payload = self._extract_visible_member_candidates(anchor_user_id)
        items = payload.get("items", []) if isinstance(payload, dict) else (payload or [])
        anchor_found = payload.get("anchorFound", True) if isinstance(payload, dict) else True

        if require_after_anchor and anchor_user_id and not anchor_found:
            return [], False

        for item in items:
            target = self._build_member_target(item)
            if not target:
                continue
            if target["profile_url"] == current_user_url:
                continue
            if target["profile_url"] in seen_urls:
                continue
            seen_urls.add(target["profile_url"])
            targets.append(target)

        return targets, anchor_found

    def scroll_member_list(self):
        moved_container = self._scroll_member_container(step_ratio=0.35, min_px=360, smooth=True)
        if moved_container is not None:
            return moved_container
        return self.scroll_page(step_ratio=0.35, min_px=360, smooth=True)

    def scroll_to_member(self, abs_top=None, offset=160):
        if abs_top is None:
            return
        try:
            target = max(0, int(abs_top) - int(offset))
        except Exception:
            return
        self.page.evaluate("(y) => window.scrollTo(0, y)", target)
        time.sleep(random.uniform(0.2, 0.5))

    def scroll_page(self, step_ratio=0.85, min_px=900, smooth=False):
        before = self.page.evaluate("window.scrollY")
        scroll_mode = "smooth" if smooth else "auto"
        self.page.evaluate(
            "(params) => { const { ratio, minPx, mode } = params; const step = Math.max(window.innerHeight * ratio, minPx); window.scrollBy({ top: step, behavior: mode }); }",
            {"ratio": step_ratio, "minPx": min_px, "mode": scroll_mode},
        )
        time.sleep(random.uniform(0.15, 0.35))
        self._wait_for_scroll_stable(timeout=2.2)
        after = self.page.evaluate("window.scrollY")
        return after > before + 5

    def _scroll_member_container(self, step_ratio=0.35, min_px=360, smooth=True):
        try:
            return self.page.evaluate(
                """(params) => {
                    const { ratio, minPx, mode } = params;
                    const sectionLabels = ['小组新人', '最近加入', 'New to the group', 'New members', 'Recently joined', 'New member', 'Recently added', 'สมาชิกใหม่', 'สมาชิกใหม่ในกลุ่ม', 'เพิ่งเข้าร่วม', 'เข้าร่วมเมื่อไม่นานมานี้', 'Thành viên mới', 'Mới tham gia', 'Vừa tham gia', 'Anggota baru', 'Baru bergabung', 'Bergabung baru-baru ini', 'Ahli baharu', 'Baru menyertai', 'Menyertai baru-baru ini', 'Nuevos miembros', 'Nuevos en el grupo', 'Recién incorporados', 'Novos membros', 'Novo no grupo', 'Ingressou recentemente', 'Nouveaux membres', 'Nouveau dans le groupe', 'A rejoint récemment', 'Neue Mitglieder', 'Neu in der Gruppe', 'Kürzlich beigetreten', 'Nuovi membri', 'Nuovo nel gruppo', 'Iscritto di recente', 'Nieuwe leden', 'Nieuw in de groep', 'Recent toegetreden', 'Новые участники', 'Новый участник', 'Недавно присоединился', 'Nowi członkowie', 'Nowy w grupie', 'Dołączył niedawno', 'Yeni üyeler', 'Yeni üye', 'Yeni katıldı', 'أعضاء جدد', 'عضو جديد', 'انضم حديثًا', 'नए सदस्य', 'नया सदस्य', 'हाल ही में शामिल हुए', '新しいメンバー', '新メンバー', '最近参加', '새 멤버', '새 회원', '최근 가입', 'สมาชิกใหม่', 'ສະມາຊິກໃໝ່', 'Mga bagong miyembro', 'Bagong miyembro', 'Kamakailan lang sumali'];
                    let sectionRoot = null;
                    for (const label of sectionLabels) {
                        const heading = Array.from(document.querySelectorAll('h2, h3, div, span'))
                            .find((el) => {
                                const text = (el.innerText || '').trim();
                                return text === label || text.includes(label);
                            });
                        if (!heading) continue;
                        let node = heading;
                        for (let depth = 0; depth < 6 && node; depth += 1) {
                            if (node.querySelector && node.querySelector('a[href*="/groups/"][href*="/user/"]')) {
                                sectionRoot = node;
                                break;
                            }
                            node = node.parentElement;
                        }
                        if (sectionRoot) break;
                    }
                    const scopeRoot = sectionRoot || document;
                    const candidates = Array.from(scopeRoot.querySelectorAll('div, section, ul')).filter((el) => {
                        if (!el) return false;
                        const style = window.getComputedStyle(el);
                        if (!style) return false;
                        const overflowY = style.overflowY || style.overflow;
                        if (!['auto', 'scroll'].includes(overflowY)) return false;
                        if (el.scrollHeight <= el.clientHeight + 5) return false;
                        if (el.clientHeight < 200) return false;
                        return true;
                    });
                    if (!candidates.length) return null;
                    candidates.sort((a, b) => b.clientHeight - a.clientHeight);
                    const container = candidates[0];
                    const before = container.scrollTop;
                    const step = Math.max(container.clientHeight * ratio, minPx);
                    container.scrollTo({ top: before + step, behavior: mode });
                    return container.scrollTop > before + 5;
                }""",
                {"ratio": step_ratio, "minPx": min_px, "mode": "smooth" if smooth else "auto"},
            )
        except Exception:
            return None

    def _wait_for_scroll_stable(self, timeout=2.0, stable_rounds=3, interval=0.12):
        end_time = time.time() + max(0.2, timeout)
        try:
            last_y = self.page.evaluate("window.scrollY")
        except Exception:
            return
        stable = 0
        while time.time() < end_time:
            time.sleep(interval)
            try:
                current_y = self.page.evaluate("window.scrollY")
            except Exception:
                return
            if abs(current_y - last_y) <= 1:
                stable += 1
                if stable >= stable_rounds:
                    return
            else:
                stable = 0
            last_y = current_y

    def scroll_to_top(self):
        self.page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.4)

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

    def _goto(self, url, label, settle_delay_range=(1.0, 1.6), networkidle_timeout=3000):
        if self._is_same_destination(self.page.url, url):
            return

        log.info(f"正在打开{label}: {url}")
        try:
            self.page.goto(url, wait_until="commit", timeout=20000)
        except Exception as exc:
            log.warning(f"{label}加载较慢，继续等待页面元素: {exc}")

        time.sleep(random.uniform(*settle_delay_range))
        try:
            self.page.wait_for_load_state("domcontentloaded", timeout=networkidle_timeout)
        except Exception:
            pass

    def _extract_joined_group_urls(self):
        return self.page.evaluate(
            """(blacklist) => {
                const sectionLabels = [
                    "你加入的小组",
                    "已加入的小组",
                    "你的小组",
                    "我的小组",
                    "Your groups",
                    "Groups you have joined",
                    "Groups you've joined",
                    "Joined groups",
                    "กลุ่มของคุณ",
                    "กลุ่มที่คุณเข้าร่วม"
                ];
                const exactSkips = new Set([
                    "查看全部",
                    "See all",
                    "新建小组",
                    "Create new group",
                    "你的小组",
                    "Your groups",
                    "发现小组",
                    "Discover groups"
                ]);

                function collectFromContainer(container) {
                    const items = [];
                    for (const a of Array.from(container.querySelectorAll('a[href*="/groups/"]'))) {
                        const href = a.getAttribute('href') || '';
                        let url;
                        try {
                            url = new URL(href, location.origin);
                        } catch {
                            continue;
                        }

                        const parts = url.pathname.split('/').filter(Boolean);
                        if (parts.length !== 2 || parts[0] !== 'groups') continue;
                        if (blacklist.includes(parts[1])) continue;

                        const rect = a.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.right < 0 || rect.bottom < 0 || rect.top > window.innerHeight) continue;

                        const text = (a.innerText || a.getAttribute('aria-label') || '')
                            .trim()
                            .replace(/\\s+/g, ' ');
                        if (!text || text.length < 2) continue;
                        if (exactSkips.has(text)) continue;

                        items.push({ href: url.href, text, top: rect.top, left: rect.left });
                    }
                    return items;
                }

                const grouped = [];
                const seen = new Set();

                for (const el of Array.from(document.querySelectorAll('h1, h2, h3, h4, span, div'))) {
                    const label = (el.innerText || '').trim().replace(/\\s+/g, ' ');
                    if (!sectionLabels.includes(label)) continue;

                    let container = el;
                    for (let depth = 0; depth < 6 && container; depth += 1) {
                        const links = collectFromContainer(container);
                        if (links.length) {
                            for (const item of links) {
                                if (seen.has(item.href)) continue;
                                seen.add(item.href);
                                grouped.push(item);
                            }
                            break;
                        }
                        container = container.parentElement;
                    }
                }

                if (!grouped.length) {
                    const sidebarMaxX = Math.min(window.innerWidth * 0.38, 480);
                    const allVisibleGroups = collectFromContainer(document);
                    for (const item of allVisibleGroups) {
                        if (item.left > sidebarMaxX) continue;
                        if (seen.has(item.href)) continue;
                        seen.add(item.href);
                        grouped.push(item);
                    }

                    if (!grouped.length) {
                        const mainMinX = Math.max(window.innerWidth * 0.12, 120);
                        for (const item of allVisibleGroups) {
                            if (item.left < mainMinX) continue;
                            if (seen.has(item.href)) continue;
                            seen.add(item.href);
                            grouped.push(item);
                        }
                    }
                }

                grouped.sort((a, b) => a.top - b.top || a.left - b.left);
                return grouped;
            }""",
            list(self.GROUP_ROUTE_BLACKLIST),
        )

    def _open_joined_groups_listing(self):
        try:
            clicked = self.page.evaluate(
                """() => {
                    const labels = new Set(['查看全部', 'See all']);
                    const sidebarMaxX = Math.min(window.innerWidth * 0.42, 520);
                    for (const el of Array.from(document.querySelectorAll('a[href], div[role="link"], span[role="link"]'))) {
                        const text = (el.innerText || el.getAttribute('aria-label') || '')
                            .trim()
                            .replace(/\\s+/g, ' ');
                        if (!labels.has(text)) continue;
                        const rect = el.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (rect.right < 0 || rect.bottom < 0 || rect.top > window.innerHeight) continue;
                        if (rect.left > sidebarMaxX) continue;
                        el.click();
                        return true;
                    }
                    return false;
                }"""
            )
            if clicked:
                time.sleep(random.uniform(1.0, 1.5))
                self._wait_for_groups_list_ready(timeout=4.0)
        except Exception:
            return

    def _wait_for_groups_list_ready(self, timeout=4.0):
        try:
            self.page.wait_for_function(
                "() => !!document.querySelector('a[href*=\"/groups/\"]')",
                timeout=int(max(0.5, timeout) * 1000),
            )
        except Exception:
            pass

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
                },
            )
        except Exception:
            return None

    def _extract_visible_member_candidates(self, anchor_user_id=None):
        if self.page.is_closed():
            return {"items": [], "anchorFound": False}
        for attempt in range(3):
            try:
                return self.page.evaluate(
                    """(anchorUserId) => {
                const recentJoinRe = /(刚加入|刚刚加入|加入时间[:：]\\s*\\S+|(约|大约|大概)?\\s*\\d+\\s*(秒|分钟|小时|天|周|月|个月|年)前(?:加入)?|\\d{4}年\\d{1,2}月\\d{1,2}日由.+添加|joined\\s+(just now|just|\\d+\\s*(second|minute|hour|day|week|month|year)s?\\s+ago|on\\s+\\w+)|added\\s+by|se\\s+unió|hace\\s+\\d+|añadido\\s+por|entrou|há\\s+\\d+|adicionado\\s+por|a\\s+rejoint|il\\s+y\\s+a|ajouté\\s+par|beigetreten|vor\\s+\\d+|si\\s+è\\s+unito|fa\\s+\\d+|aggiunto\\s+da|toegetreden|geleden|присоединился|назад|dołączył|temu|katıldı|önce|انضم|منذ|शामिल|पहले|bergabung|yang\\s+lalu|menyertai|yang\\s+lalu|tham\\s+gia|trước|参加|前|가입|전|เพิ่งเข้าร่วม|เข้าร่วมเมื่อ|เข้าร่วม|เมื่อ\\s*\\d+\\s*(วินาที|นาที|ชั่วโมง|วัน|สัปดาห์|เดือน|ปี)ที่แล้ว|ចូលរួម|ເຂົ້າຮ່ວມ|sumali|nakaraan)/i;
                const groupUserRe = /\\/groups\\/[^/]+\\/user\\/\\d+\\/?/i;
                const minX = window.innerWidth * 0.28;
                const maxX = window.innerWidth * 0.82;
                const skipNames = new Set(['查看全部', 'See all', '邀请', 'Invite', '发消息', 'Message', '更多', 'More']);
                const results = [];
                const sectionLabels = ['小组新人', '最近加入', 'New to the group', 'New members', 'Recently joined', 'New member', 'Recently added', 'สมาชิกใหม่', 'สมาชิกใหม่ในกลุ่ม', 'เพิ่งเข้าร่วม', 'เข้าร่วมเมื่อไม่นานมานี้', 'Thành viên mới', 'Mới tham gia', 'Vừa tham gia', 'Anggota baru', 'Baru bergabung', 'Bergabung baru-baru ini', 'Ahli baharu', 'Baru menyertai', 'Menyertai baru-baru ini', 'Nuevos miembros', 'Nuevos en el grupo', 'Recién incorporados', 'Novos membros', 'Novo no grupo', 'Ingressou recentemente', 'Nouveaux membres', 'Nouveau dans le groupe', 'A rejoint récemment', 'Neue Mitglieder', 'Neu in der Gruppe', 'Kürzlich beigetreten', 'Nuovi membri', 'Nuovo nel gruppo', 'Iscritto di recente', 'Nieuwe leden', 'Nieuw in de groep', 'Recent toegetreden', 'Новые участники', 'Новый участник', 'Недавно присоединился', 'Nowi członkowie', 'Nowy w grupie', 'Dołączył niedawno', 'Yeni üyeler', 'Yeni üye', 'Yeni katıldı', 'أعضاء جدد', 'عضو جديد', 'انضم حديثًا', 'नए सदस्य', 'नया सदस्य', 'हाल ही में शामिल हुए', '新しいメンバー', '新メンバー', '最近参加', '새 멤버', '새 회원', '최근 가입', 'សមាជិកថ្មី', 'ສະມາຊິກໃໝ່', 'Mga bagong miyembro', 'Bagong miyembro', 'Kamakailan lang sumali'];
                let sectionRoot = null;
                for (const label of sectionLabels) {
                    const heading = Array.from(document.querySelectorAll('h2, h3, div, span'))
                        .find((el) => {
                            const text = (el.innerText || '').trim();
                            return text === label || text.includes(label);
                        });
                    if (!heading) continue;
                    let node = heading;
                    for (let depth = 0; depth < 6 && node; depth += 1) {
                        if (node.querySelector && node.querySelector('a[href*="/groups/"][href*="/user/"]')) {
                            sectionRoot = node;
                            break;
                        }
                        node = node.parentElement;
                    }
                    if (sectionRoot) break;
                }
                const scopeRoot = sectionRoot || document;

                let order = 0;
                for (const a of Array.from(scopeRoot.querySelectorAll('a[href]'))) {
                    const href = a.getAttribute('href') || '';
                    if (!href || href.startsWith('#')) continue;
                    if (href.includes('/groups/') && !groupUserRe.test(href)) continue;

                    const rect = a.getBoundingClientRect();
                    if (!rect.width || !rect.height) continue;
                    if (rect.bottom < 0 || rect.top > window.innerHeight) continue;
                    const centerX = rect.left + rect.width / 2;
                    if (centerX < minX || centerX > maxX) continue;

                    const name = (a.innerText || a.getAttribute('aria-label') || '')
                        .trim()
                        .replace(/\\s+/g, ' ');
                    if (!name || name.length < 2 || name.length > 80) continue;
                    if (skipNames.has(name)) continue;

                    let card = a;
                    for (let depth = 0; depth < 7 && card; depth += 1) {
                        const text = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                        if (text && text.length <= 260 && text.includes(name)) {
                            if (sectionRoot) {
                                break;
                            }
                            if (recentJoinRe.test(text)) {
                                break;
                            }
                        }
                        card = card.parentElement;
                    }

                    if (!card) continue;
                    const cardText = (card.innerText || '').trim().replace(/\\s+/g, ' ');
                    if (!cardText.includes(name)) continue;
                    if (!sectionRoot && !recentJoinRe.test(cardText)) continue;

                    results.push({
                        href,
                        name,
                        cardText: cardText.slice(0, 200),
                        top: rect.top,
                        left: rect.left,
                        absTop: rect.top + window.scrollY,
                        absLeft: rect.left + window.scrollX,
                        order: order++
                    });
                }

                results.sort((a, b) => a.top - b.top || a.left - b.left || a.order - b.order);
                if (!anchorUserId) {
                    return { items: results, anchorFound: true };
                }
                const anchorToken = String(anchorUserId);
                const anchorIndex = results.findIndex((item) => {
                    const href = item.href || '';
                    return href.includes(`/user/${anchorToken}`) || href.includes(`id=${anchorToken}`);
                });
                if (anchorIndex >= 0) {
                    return { items: results.slice(anchorIndex + 1), anchorFound: true };
                }
                return { items: [], anchorFound: false };
            }""",
                    anchor_user_id or "",
                )
            except Exception as exc:
                message = str(exc)
                if "Execution context was destroyed" in message or "Target page, context or browser has been closed" in message:
                    if attempt == 0:
                        log.warning("成员列表页面发生跳转，重试抓取成员列表...")
                    time.sleep(0.2 + 0.2 * attempt)
                    continue
                raise
        return {"items": [], "anchorFound": False}

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
            "card_text": item.get("cardText", ""),
            "abs_top": item.get("absTop"),
            "abs_left": item.get("absLeft"),
        }

    def _extract_group_root_url(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url))
        segments = [segment for segment in parsed.path.split("/") if segment]
        if len(segments) >= 2 and segments[0] == "groups":
            return f"https://www.facebook.com/groups/{segments[1]}"
        return None

    def _is_group_members_page(self, url):
        parsed = urlparse(urljoin("https://www.facebook.com", url))
        return parsed.path.startswith("/groups/") and "/members" in parsed.path

    def _is_same_destination(self, current_url, target_url):
        current = urlparse(urljoin("https://www.facebook.com", current_url or ""))
        target = urlparse(urljoin("https://www.facebook.com", target_url or ""))
        current_path = current.path.rstrip("/")
        target_path = target.path.rstrip("/")
        return bool(current_path and current_path == target_path)

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
