import time
from urllib.parse import urlparse

import requests

from utils.logger import log


class BrowserManager:
    def __init__(self, settings):
        self.settings = settings
        self.api_url = (settings.get("bit_api") or "").rstrip("/")
        api_token = settings.get("api_token") or ""
        self.headers = {"x-api-key": api_token} if api_token else {}
        self.open_timeout_seconds = int(settings.get("bitbrowser_open_timeout_seconds", 45) or 45)
        self.active_poll_seconds = int(settings.get("bitbrowser_active_poll_seconds", 35) or 35)
        self.active_poll_interval_seconds = float(
            settings.get("bitbrowser_active_poll_interval_seconds", 1.5) or 1.5
        )
        self.open_retry_count = max(1, int(settings.get("bitbrowser_open_retry_count", 2) or 2))

    def open_for_browser(self, playwright, browser_id=None):
        return self._open_bitbrowser(playwright, browser_id=browser_id)

    def close(self, session):
        if not session:
            return

        try:
            session["browser"].close()
        except Exception as exc:
            log.warning(f"关闭 BitBrowser 会话失败: {exc}")

    def _open_bitbrowser(self, playwright, browser_id=None):
        browser_id = browser_id or self._pick_default_bitbrowser_id()
        if not browser_id:
            log.error("未获取到可用的 BitBrowser 窗口，无法连接。")
            return None

        endpoint = self.start(browser_id)
        if not endpoint:
            return None

        browser = self._connect_over_cdp_with_retry(playwright, endpoint, browser_id=browser_id)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = self._pick_page(context)
        return {
            "browser": browser,
            "context": context,
            "page": page,
            "account_id": browser_id,
        }

    def list_bitbrowser_profiles(self, page_size=100):
        url = f"{self.api_url}/browser/list"
        profiles = []
        page = 0

        while True:
            try:
                res = requests.post(
                    url,
                    json={"page": page, "pageSize": page_size},
                    headers=self.headers,
                    timeout=10,
                ).json()
            except Exception as exc:
                log.error(f"获取 BitBrowser 窗口列表失败: {exc}")
                return []

            if not (res.get("success") is True or res.get("code") == 0):
                log.error(f"获取 BitBrowser 窗口列表失败: {res.get('msg')}")
                return []
            data = res.get("data") or {}
            items = data.get("list") or []
            for item in items:
                browser_id = item.get("id")
                if not browser_id or item.get("isDelete") == 1:
                    continue
                profiles.append(
                    {
                        "id": browser_id,
                        "name": item.get("name") or "",
                        "seq": item.get("seq"),
                        "status": item.get("status"),
                    }
                )

            total_num = int(data.get("totalNum") or 0)
            if len(profiles) >= total_num or not items:
                break
            page += 1

        return profiles

    def _pick_default_bitbrowser_id(self):
        profiles = self.list_bitbrowser_profiles(page_size=1)
        first_profile = profiles[0] if profiles else None
        return first_profile.get("id") if first_profile else None

    def _pick_page(self, context):
        for page in context.pages:
            if not page.url.startswith("chrome://"):
                return page
        return context.new_page()

    def start(self, browser_id):
        url = f"{self.api_url}/browser/open"
        payload = {"id": browser_id}
        last_error = None

        existing_endpoint = self.get_active_endpoint(browser_id, wait_seconds=2)
        if existing_endpoint:
            log.info(f"窗口 {browser_id} 已存在活动连接，直接复用。")
            return existing_endpoint

        for attempt in range(1, self.open_retry_count + 1):
            try:
                log.info(f"正在通过 API 开启窗口: {browser_id} ... (尝试 {attempt}/{self.open_retry_count})")
                res = requests.post(
                    url,
                    json=payload,
                    headers=self.headers,
                    timeout=self.open_timeout_seconds,
                ).json()

                if res.get("success") is True or res.get("code") == 0:
                    endpoint = ((res.get("data") or {}).get("http") or "").strip()
                    if endpoint:
                        log.success(f"窗口 {browser_id} 开启成功")
                        return endpoint

                    log.warning(f"窗口 {browser_id} 已返回成功，但暂未拿到连接地址，继续轮询活动连接...")
                    endpoint = self.get_active_endpoint(browser_id, wait_seconds=self.active_poll_seconds)
                    if endpoint:
                        log.success(f"窗口 {browser_id} 活动连接就绪")
                        return endpoint

                    last_error = "active_endpoint_missing_after_open"
                else:
                    last_error = res.get("msg") or "open_failed"
                    log.warning(f"窗口 {browser_id} 开启响应异常: {last_error}")
            except requests.exceptions.Timeout:
                last_error = "open_timeout"
                log.warning(f"窗口 {browser_id} 开启超时，继续轮询活动连接...")
            except Exception as exc:
                last_error = repr(exc)
                log.warning(f"窗口 {browser_id} 开启异常: {exc}")

            endpoint = self.get_active_endpoint(browser_id, wait_seconds=self.active_poll_seconds)
            if endpoint:
                log.success(f"窗口 {browser_id} 活动连接就绪")
                return endpoint

            if attempt < self.open_retry_count:
                time.sleep(2)

        log.error(f"窗口 {browser_id} 无法建立活动连接: {last_error}")
        return None

    def get_active_endpoint(self, browser_id, wait_seconds=5):
        url = f"{self.api_url}/browser/active"
        deadline = time.time() + max(wait_seconds, 0)
        while time.time() <= deadline:
            try:
                res = requests.post(
                    url,
                    json={"id": browser_id},
                    headers=self.headers,
                    timeout=8,
                ).json()
                if res.get("success") is True or res.get("code") == 0:
                    endpoint = ((res.get("data") or {}).get("http") or "").strip()
                    if endpoint:
                        return endpoint
            except Exception:
                pass

            if time.time() >= deadline:
                break
            time.sleep(self.active_poll_interval_seconds)
        return None

    def _normalize_cdp_endpoint(self, endpoint):
        normalized = (endpoint or "").strip()
        if not normalized:
            return normalized
        if not (normalized.startswith("http://") or normalized.startswith("https://")):
            normalized = f"http://{normalized}"

        parsed = urlparse(normalized)
        if parsed.hostname in ("127.0.0.1", "localhost"):
            api_host = self._get_api_host()
            if api_host and api_host not in ("127.0.0.1", "localhost"):
                netloc = f"{api_host}:{parsed.port}" if parsed.port else api_host
                return parsed._replace(netloc=netloc).geturl()
        return normalized

    def _get_api_host(self):
        if not self.api_url:
            return ""
        return urlparse(self.api_url).hostname or ""

    def _connect_over_cdp_with_retry(self, playwright, endpoint, browser_id=None):
        normalized = self._normalize_cdp_endpoint(endpoint)
        retry_count = int(self.settings.get("bitbrowser_cdp_connect_retry_count", 80) or 80)
        retry_delay = float(self.settings.get("bitbrowser_cdp_connect_retry_delay", 0.6) or 0.6)
        refresh_every = int(self.settings.get("bitbrowser_cdp_refresh_every", 6) or 6)
        last_exc = None
        for attempt in range(1, max(1, retry_count) + 1):
            try:
                return playwright.chromium.connect_over_cdp(normalized)
            except Exception as exc:
                last_exc = exc
                msg = str(exc)
                if "ECONNREFUSED" in msg or "connect ECONNREFUSED" in msg:
                    if browser_id and refresh_every > 0 and attempt % refresh_every == 0:
                        refreshed = self.get_active_endpoint(browser_id, wait_seconds=2)
                        if refreshed and refreshed != endpoint:
                            endpoint = refreshed
                            normalized = self._normalize_cdp_endpoint(refreshed)
                    if attempt < retry_count:
                        time.sleep(retry_delay)
                        continue
                raise
        raise last_exc
