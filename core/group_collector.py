from playwright.sync_api import sync_playwright

from core.browser_manager import BrowserManager
from core.scraper import Scraper
from utils.logger import log


def _resolve_window_name(profile, window_token="", browser_id=""):
    profile = profile or {}
    name = str(profile.get("name") or "").strip()
    if name:
        return name
    seq = profile.get("seq")
    if seq not in (None, ""):
        return f"窗口{seq}"
    token = str(window_token or "").strip()
    if token:
        return f"窗口{token}"
    browser_value = str(browser_id or profile.get("id") or "").strip()
    return browser_value


def _emit_collect_log(log_callback, message):
    text = str(message or "").strip()
    if not text:
        return
    log.info(text)
    if callable(log_callback):
        try:
            log_callback(text)
        except Exception:
            pass


def _normalize_window_groups(groups):
    normalized = []
    for item in groups or []:
        group_id = str((item or {}).get("group_id") or "").strip()
        group_name = str((item or {}).get("group_name") or "").strip()
        group_url = str((item or {}).get("group_url") or "").strip()
        if not (group_id and group_url):
            continue
        normalized.append(
            {
                "group_id": group_id,
                "group_name": group_name,
                "group_url": group_url,
            }
        )
    return normalized


def collect_groups_for_machine(browser_settings, max_scrolls=8, log_callback=None, profiles=None):
    # Reuse the current main-chain helpers so window token and profile order stay identical.
    from main import format_account_label, resolve_window_token, sort_bitbrowser_profiles

    bm = BrowserManager(browser_settings)
    target_profiles = sort_bitbrowser_profiles(
        profiles if profiles is not None else bm.list_bitbrowser_profiles()
    )
    if not target_profiles:
        _emit_collect_log(log_callback, "[采集] 未获取到任何可用的 BitBrowser 窗口。")
        return {
            "windows": [],
            "profile_count": 0,
            "collected_window_count": 0,
            "failed_window_count": 0,
            "total_groups": 0,
        }

    windows = []
    total_groups = 0
    collected_window_count = 0
    failed_window_count = 0
    _emit_collect_log(
        log_callback,
        f"[采集] 已识别 {len(target_profiles)} 个窗口，开始逐窗口采集已加入小组。",
    )

    with sync_playwright() as playwright:
        for profile in target_profiles:
            session = None
            browser_id = str((profile or {}).get("id") or "").strip()
            window_token = resolve_window_token(profile)
            window_name = _resolve_window_name(
                profile,
                window_token=window_token,
                browser_id=browser_id,
            )
            window_label = format_account_label(profile) or (window_token or browser_id or "未知窗口")
            try:
                _emit_collect_log(log_callback, f"[采集] 正在采集 {window_label} ...")
                session = bm.open_for_browser(playwright, browser_id=browser_id or None)
                if not session:
                    failed_window_count += 1
                    windows.append(
                        {
                            "window_token": window_token,
                            "window_name": window_name,
                            "groups": [],
                            "error": "session_open_failed",
                        }
                    )
                    _emit_collect_log(log_callback, f"[采集] {window_label} 连接失败，跳过该窗口。")
                    continue

                page = session.get("page")
                scraper = Scraper(page, window_label=window_label)
                groups = _normalize_window_groups(
                    scraper.collect_joined_groups(max_scrolls=max_scrolls)
                )
                total_groups += len(groups)
                collected_window_count += 1
                windows.append(
                    {
                        "window_token": window_token or str(session.get("account_id") or "").strip(),
                        "window_name": window_name,
                        "groups": groups,
                    }
                )
                _emit_collect_log(
                    log_callback,
                    f"[采集] {window_label} 采集完成，共获取 {len(groups)} 个小组。",
                )
            except Exception as exc:
                failed_window_count += 1
                windows.append(
                    {
                        "window_token": window_token,
                        "window_name": window_name,
                        "groups": [],
                        "error": str(exc),
                    }
                )
                _emit_collect_log(log_callback, f"[采集] {window_label} 采集失败: {exc}")
            finally:
                session = None

    _emit_collect_log(
        log_callback,
        f"[采集] 采集结束：成功窗口 {collected_window_count} 个，失败窗口 {failed_window_count} 个，累计小组 {total_groups} 个。",
    )
    return {
        "windows": windows,
        "profile_count": len(target_profiles),
        "collected_window_count": collected_window_count,
        "failed_window_count": failed_window_count,
        "total_groups": total_groups,
    }
