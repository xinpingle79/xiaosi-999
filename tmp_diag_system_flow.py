import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from playwright.sync_api import sync_playwright

from core.browser_manager import BrowserManager
from core.messager import Messager
from core.scraper import Scraper


BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIAG_DIR = BASE_DIR / "runtime" / "diag"
RUNTIME_DIAG_DIR.mkdir(parents=True, exist_ok=True)


def _load_client_config() -> Dict[str, Any]:
    return yaml.safe_load((BASE_DIR / "config" / "client.yaml").read_text(encoding="utf-8")) or {}


def _load_latest_selected_group() -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[Path]]:
    files = sorted(
        (BASE_DIR / "runtime" / "data").glob("selected_window_groups_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for window_token, groups in payload.items():
            if not isinstance(groups, list) or not groups:
                continue
            first_group = groups[0] or {}
            group_url = str(first_group.get("group_url") or "").strip()
            if group_url:
                return str(window_token or "").strip(), first_group, path
    return None, None, None


def _pick_profile_by_window_token(profiles: List[Dict[str, Any]], window_token: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized = str(window_token or "").strip()
    if normalized.isdigit():
        target_seq = int(normalized)
        for profile in profiles:
            if int(profile.get("seq") or 0) == target_seq:
                return profile
    return profiles[0] if profiles else None


def _safe_title(page) -> str:
    try:
        return str(page.title() or "")
    except Exception:
        return ""


def _safe_url(page) -> str:
    try:
        return str(page.url or "")
    except Exception:
        return ""


def _safe_box(locator) -> Optional[Dict[str, Any]]:
    try:
        return locator.bounding_box()
    except Exception:
        return None


def _safe_inner_text(locator) -> str:
    try:
        return str(locator.inner_text(timeout=800) or "").strip()
    except Exception:
        return ""


def _safe_attr(locator, name: str) -> str:
    try:
        return str(locator.get_attribute(name, timeout=800) or "").strip()
    except Exception:
        return ""


def _dump_right_chat_dom(page) -> Dict[str, Any]:
    return page.evaluate(
        """() => {
            const norm = (v) => (v || '').replace(/\\s+/g, ' ').trim();
            const visible = (el) => {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                if (!rect.width || !rect.height) return false;
                const style = window.getComputedStyle(el);
                if (!style) return false;
                if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                return true;
            };
            const viewportW = window.innerWidth;
            const viewportH = window.innerHeight;
            const editors = Array.from(document.querySelectorAll('[contenteditable="true"], div[role="textbox"], div[contenteditable="plaintext-only"]'))
                .filter(visible)
                .map((el, idx) => {
                    const rect = el.getBoundingClientRect();
                    return {
                        idx,
                        x: rect.x,
                        y: rect.y,
                        w: rect.width,
                        h: rect.height,
                        ariaLabel: norm(el.getAttribute('aria-label')),
                        text: norm(el.innerText).slice(0, 200),
                        rightSide: rect.x > viewportW * 0.35,
                    };
                });
            const buttons = Array.from(document.querySelectorAll('button, [role="button"], [aria-label], a'))
                .filter(visible)
                .map((el, idx) => {
                    const rect = el.getBoundingClientRect();
                    return {
                        idx,
                        x: rect.x,
                        y: rect.y,
                        w: rect.width,
                        h: rect.height,
                        label: norm(el.getAttribute('aria-label')).slice(0, 200),
                        text: norm(el.innerText || el.textContent || '').slice(0, 200),
                    };
                })
                .filter((item) => item.x > viewportW * 0.55)
                .slice(0, 120);
            const bubbleCandidates = Array.from(document.querySelectorAll('div, span'))
                .filter(visible)
                .map((el, idx) => {
                    const rect = el.getBoundingClientRect();
                    const text = norm(el.innerText || el.textContent || '');
                    if (!text || text.length > 160) return null;
                    if (rect.x < viewportW * 0.35) return null;
                    if (rect.width < 24 || rect.height < 10) return null;
                    return {
                        idx,
                        x: rect.x,
                        y: rect.y,
                        w: rect.width,
                        h: rect.height,
                        text,
                    };
                })
                .filter(Boolean)
                .slice(0, 120);
            return {
                href: location.href,
                title: document.title,
                viewportW,
                viewportH,
                editors,
                buttons,
                bubbleCandidates,
            };
        }"""
    )


def _locator_signature(locator) -> Dict[str, Any]:
    return {
        "text": _safe_inner_text(locator)[:300],
        "aria_label": _safe_attr(locator, "aria-label"),
        "role": _safe_attr(locator, "role"),
        "href": _safe_attr(locator, "href"),
        "box": _safe_box(locator),
    }


def _normalize_name(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _match_target_from_payload(visible_payload: Dict[str, Any], normalized_hint: str) -> Optional[Dict[str, Any]]:
    for bucket_name in ("targets", "fallback_targets"):
        for item in list((visible_payload or {}).get(bucket_name) or []):
            if normalized_hint in _normalize_name(item.get("name") or ""):
                return item
    return None


def _collect_targets_for_scan(
    scraper: Scraper,
    visible_payload: Dict[str, Any],
    max_targets: int,
    max_scroll_rounds: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    collected: List[Dict[str, Any]] = []
    seen_keys = set()
    steps: List[Dict[str, Any]] = []
    scroll_round = 0

    while True:
        current_batch: List[Dict[str, Any]] = []
        for bucket_name in ("targets", "fallback_targets"):
            current_batch.extend(list((visible_payload or {}).get(bucket_name) or []))

        added = 0
        for item in current_batch:
            key = (
                str(item.get("user_id") or "").strip()
                or str(item.get("group_user_url") or "").strip()
                or str(item.get("profile_url") or "").strip()
                or _normalize_name(item.get("name") or "")
            )
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            collected.append(item)
            added += 1
            if len(collected) >= max_targets:
                break

        steps.append(
            {
                "step": "scan_collect_targets_round",
                "round": scroll_round,
                "added": added,
                "collected": len(collected),
            }
        )

        if len(collected) >= max_targets or scroll_round >= max_scroll_rounds:
            break

        moved = scraper.scroll_member_list()
        scroll_round += 1
        steps.append(
            {
                "step": "scan_scroll_member_list",
                "round": scroll_round,
                "moved": bool(moved),
            }
        )
        if not moved:
            break
        visible_payload = scraper.collect_visible_new_member_targets()

    return collected, steps


def main() -> int:
    client_cfg = _load_client_config()
    task_cfg = client_cfg.get("task_settings") or {}
    target_name_hint = (
        str(os.environ.get("DIAG_TARGET_NAME") or "").strip()
        or (str(sys.argv[1]).strip() if len(sys.argv) > 1 else "")
    )

    report: Dict[str, Any] = {
        "selected_group_source": None,
        "selected_group": None,
        "profile": None,
        "target_name_hint": target_name_hint,
        "steps": [],
    }

    window_token, group_item, source_path = _load_latest_selected_group()
    if not group_item:
        report["error"] = "no_selected_group_file"
        (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    report["selected_group_source"] = str(source_path)
    report["selected_group"] = group_item

    bm = BrowserManager(client_cfg)
    profiles = bm.list_bitbrowser_profiles(page_size=20)
    profile = _pick_profile_by_window_token(profiles, window_token)
    if not profile:
        report["error"] = "no_profile_available"
        (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 1

    report["profile"] = {
        "id": profile.get("id"),
        "seq": profile.get("seq"),
        "status": profile.get("status"),
        "window_token": window_token,
    }

    with sync_playwright() as playwright:
        session = bm.open_for_browser(playwright, browser_id=profile["id"])
        if not session:
            report["error"] = "bitbrowser_session_open_failed"
            (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 1

        page = session["page"]
        window_label = f"窗口{profile.get('seq') or profile.get('id')}"
        scraper = Scraper(page, window_label=window_label)
        sender = Messager(page, task_cfg=task_cfg, window_label=window_label)

        report["steps"].append(
            {
                "step": "page_attached",
                "url": _safe_url(page),
                "title": _safe_title(page),
            }
        )

        group_url = str(group_item.get("group_url") or "").strip()
        open_members_ok = scraper.open_group_members_page(group_url)
        report["steps"].append(
            {
                "step": "open_group_members_page",
                "ok": bool(open_members_ok),
                "url": _safe_url(page),
                "title": _safe_title(page),
            }
        )
        page.screenshot(path=str(RUNTIME_DIAG_DIR / "01_members_page.png"), full_page=True)

        if not open_members_ok:
            (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 2

        focused = scraper.focus_newcomers_section(max_scroll_rounds=12)
        report["steps"].append(
            {
                "step": "focus_newcomers_section",
                "ok": bool(focused),
                "url": _safe_url(page),
            }
        )
        page.screenshot(path=str(RUNTIME_DIAG_DIR / "02_newcomers_scope.png"), full_page=True)

        visible_payload = scraper.collect_visible_new_member_targets()
        targets = list((visible_payload or {}).get("targets") or [])
        fallback_targets = list((visible_payload or {}).get("fallback_targets") or [])
        report["steps"].append(
            {
                "step": "collect_visible_new_member_targets",
                "target_count": len(targets),
                "fallback_target_count": len(fallback_targets),
                "anchor_found": bool((visible_payload or {}).get("anchor_found")),
                "sample_targets": targets[:3],
            }
        )

        candidate_targets: List[Dict[str, Any]] = []
        if target_name_hint:
            normalized_hint = _normalize_name(target_name_hint)
            scroll_round = 0
            max_scroll_rounds = 300
            while scroll_round < max_scroll_rounds:
                matched_target = _match_target_from_payload(visible_payload, normalized_hint)
                if matched_target:
                    candidate_targets = [matched_target]
                    break
                if candidate_targets:
                    break
                moved = scraper.scroll_member_list()
                scroll_round += 1
                report["steps"].append(
                    {
                        "step": "scroll_member_list_for_target_name",
                        "round": scroll_round,
                        "moved": bool(moved),
                    }
                )
                if not moved:
                    break
                visible_payload = scraper.collect_visible_new_member_targets()
            report["steps"].append(
                {
                    "step": "target_name_match_result",
                    "target_name_hint": target_name_hint,
                    "max_scroll_rounds": max_scroll_rounds,
                    "searched_rounds": scroll_round,
                    "matched": bool(candidate_targets),
                    "matched_target": candidate_targets[0] if candidate_targets else None,
                }
            )
        else:
            scan_limit = max(1, int(os.environ.get("DIAG_SCAN_LIMIT") or 30))
            scan_scroll_rounds = max(0, int(os.environ.get("DIAG_SCAN_SCROLL_ROUNDS") or 40))
            candidate_targets, scan_steps = _collect_targets_for_scan(
                scraper=scraper,
                visible_payload=visible_payload,
                max_targets=scan_limit,
                max_scroll_rounds=scan_scroll_rounds,
            )
            report["steps"].extend(scan_steps)
            report["steps"].append(
                {
                    "step": "scan_collect_targets_result",
                    "scan_limit": scan_limit,
                    "scan_scroll_rounds": scan_scroll_rounds,
                    "collected_target_count": len(candidate_targets),
                }
            )

        if not candidate_targets:
            report["error"] = "no_target_visible"
            (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 3

        cleanup_state = sender._inspect_surface_cleanup_state()
        report["steps"].append(
            {
                "step": "inspect_surface_cleanup_state",
                "state": cleanup_state,
            }
        )

        report["candidate_diagnostics"] = []
        chosen_target = None
        chat_session = None
        scan_mode = not bool(target_name_hint)

        for idx, target in enumerate(candidate_targets, start=1):
            sender._dismiss_member_hover_card()
            candidate_report: Dict[str, Any] = {
                "index": idx,
                "target": target,
            }
            link = sender._find_member_link(target)
            candidate_report["link_found"] = bool(link)
            candidate_report["link_signature"] = _locator_signature(link) if link else None
            if not link:
                report["candidate_diagnostics"].append(candidate_report)
                continue

            link_box = sender._wait_for_stable_box(link, max_rounds=2, delay=0.06)
            hover_card = sender._find_hover_card_container(link_box) if link_box else None
            if hover_card is None:
                sender._trigger_hover_card(link)
                hover_card = sender._wait_for_condition(
                    "成员资料卡",
                    "系统流程诊断等待资料卡",
                    lambda: sender._find_hover_card_container(link_box),
                    timeout_ms=max(1000, int(sender.hover_card_wait_timeout_ms * 0.65)),
                    interval=0.12,
                )
            candidate_report["hover_card_found"] = bool(hover_card)
            if hover_card:
                candidate_report["hover_card_box"] = _safe_box(hover_card)
                candidate_report["hover_card_actions"] = sender._collect_action_candidates_in_container(hover_card)
            else:
                candidate_report["hover_card_box"] = None
                candidate_report["hover_card_actions"] = []

            button, failure = sender._resolve_member_card_message_button(link, target["name"])
            candidate_report["message_button_found"] = bool(button)
            candidate_report["message_button_signature"] = _locator_signature(button) if button else None
            candidate_report["message_button_failure"] = failure
            report["candidate_diagnostics"].append(candidate_report)

            page.screenshot(
                path=str(RUNTIME_DIAG_DIR / f"03_hover_card_{idx}.png"),
                full_page=True,
            )

            if scan_mode or not button:
                continue

            sender._run_pause_aware_action(
                lambda step_timeout: button.click(timeout=step_timeout),
                timeout_ms=sender._scale_ms(2000, min_ms=1500),
            )
            report["steps"].append(
                {
                    "step": "click_message_button",
                    "target_name": target["name"],
                    "ok": True,
                }
            )

            chat_open_result = sender._wait_for_chat_open_result(
                expected_name=target["name"],
                timeout_ms=min(sender.chat_session_wait_timeout_ms, 3600),
                stage="系统流程诊断首次复核",
            )
            chat_session, restriction, chat_shell_seen = sender._extract_chat_open_result(chat_open_result)
            report["steps"].append(
                {
                    "step": "wait_for_chat_open_result",
                    "target_name": target["name"],
                    "chat_session_found": bool(chat_session),
                    "restriction": restriction,
                    "chat_shell_seen": bool(chat_shell_seen),
                    "chat_session_editor_box": _safe_box(chat_session["editor"]) if chat_session else None,
                }
            )
            page.screenshot(path=str(RUNTIME_DIAG_DIR / "04_chat_window.png"), full_page=True)
            report["chat_dom"] = _dump_right_chat_dom(page)
            chosen_target = target
            break

        if scan_mode:
            message_available = sum(1 for item in report["candidate_diagnostics"] if item.get("message_button_found"))
            report["scan_summary"] = {
                "scanned_count": len(report["candidate_diagnostics"]),
                "message_available_count": message_available,
                "message_unavailable_count": max(0, len(report["candidate_diagnostics"]) - message_available),
            }
            (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 0

        report["target"] = chosen_target

        if not chosen_target:
            report["error"] = "message_button_unavailable"
            (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 5

        if chat_session:
            editor_ready = sender._wait_for_editor_ready(
                chat_session["editor"],
                timeout_ms=min(sender.editor_ready_wait_timeout_ms, 2200),
            )
            report["steps"].append(
                {
                    "step": "wait_for_editor_ready",
                    "target_name": chosen_target["name"],
                    "ready": bool(editor_ready),
                    "editor_signature": _locator_signature(chat_session["editor"]),
                }
            )

        (RUNTIME_DIAG_DIR / "system_flow_report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
