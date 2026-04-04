const logOutput = document.getElementById("log-output");
const restartWindowToken = document.getElementById("restart-window-token");
const pageTitle = document.getElementById("page-title");
const navItems = Array.from(document.querySelectorAll(".nav-item"));
const pageCards = Array.from(document.querySelectorAll(".card[data-page]"));
const saveBtn = document.getElementById("save-btn");
const startBtn = document.getElementById("start-btn");
const stopBtn = document.getElementById("stop-btn");
const pauseBtn = document.getElementById("pause-btn");
const resumeBtn = document.getElementById("resume-btn");
const collectBtn = document.getElementById("collect-btn");
const stopCollectBtn = document.getElementById("stop-collect-btn");
const restartWindowBtn = document.getElementById("restart-window-btn");
const currentAccountName = document.getElementById("current-account-name");
const currentAccountRole = document.getElementById("current-account-role");
const logoutBtn = document.getElementById("logout-btn");

const dashSendToday = document.getElementById("dash-send-today");
const dashSendTotal = document.getElementById("dash-send-total");
const dashSendYesterday = document.getElementById("dash-send-yesterday");
const dashFailToday = document.getElementById("dash-fail-today");
const dashFailTotal = document.getElementById("dash-fail-total");
const dashFailYesterday = document.getElementById("dash-fail-yesterday");
const dashUserToday = document.getElementById("dash-user-today");
const dashUserTotal = document.getElementById("dash-user-total");
const dashUserYesterday = document.getElementById("dash-user-yesterday");
const dashActiveToday = document.getElementById("dash-active-today");
const dashActiveTotal = document.getElementById("dash-active-total");
const dashActiveYesterday = document.getElementById("dash-active-yesterday");
const logActiveCount = document.getElementById("log-active-count");
const logSentCount = document.getElementById("log-sent-count");
const logFailedCount = document.getElementById("log-failed-count");
const logWindowCount = document.getElementById("log-window-count");
const trendChart = document.getElementById("trend-chart");
const trendAxis = document.getElementById("trend-axis");
const userTableBody = document.getElementById("user-table-body");
const deviceTableBody = document.getElementById("device-table-body");
const deviceInfoTableBody = document.getElementById("device-info-table-body");
const deviceDetailMeta = document.getElementById("device-detail-meta");
const deviceDetailTableBody = document.getElementById("device-detail-table-body");
const deviceDetailBackBtn = document.getElementById("device-detail-back-btn");
const deviceDetailSaveBtn = document.getElementById("device-detail-save-btn");
const addUserBtn = document.getElementById("add-user-btn");
const userModal = document.getElementById("user-modal");
const userModalTitle = document.getElementById("user-modal-title");
const userModalUsername = document.getElementById("user-modal-username");
const userModalPassword = document.getElementById("user-modal-password");
const userModalMaxDevices = document.getElementById("user-modal-max-devices");
const userModalPlan = document.getElementById("user-modal-plan");
const userModalExpireDisplay = document.getElementById("user-modal-expire-display");
const userModalError = document.getElementById("user-modal-error");
const userModalSave = document.getElementById("user-modal-save");
const userModalCancel = document.getElementById("user-modal-cancel");
const userModalToggle = document.getElementById("user-modal-toggle");
const STATUS_POLL_INTERVAL_MS = 3000;
const AUX_REFRESH_INTERVAL_MS = 10000;
const IS_SUB = window.location.pathname.startsWith("/sub");
const API_PREFIX = IS_SUB ? "/api/sub" : "/api/admin";
let lastTrend = null;
let runtimeLogUserInteracting = false;
let tableUserInteracting = false;
let dashboardLabelsReady = false;
let latestStatusPayload = null;
let latestAgentRows = [];
let latestDeviceInfoRows = [];
let currentDeviceDetail = null;
let currentDeviceDetailVisibleWindowTokens = [];
let currentPageKey = "task";
let lastUsersRefreshAt = 0;
let lastDevicesRefreshAt = 0;
let lastDeviceInfoRefreshAt = 0;
let lastDeviceWindowTokenEditAt = 0;
const deviceWindowTokenDrafts = new Map();
const GROUP_ACTION_LABELS = new Set([
  "查看小组",
  "进入小组",
  "进入群组",
  "打开小组",
  "前往小组",
  "view group",
  "open group",
  "go to group",
  "see group",
]);

function isLogSelectionActive(target) {
  if (!target) return false;
  const selection = window.getSelection();
  if (!selection || selection.rangeCount === 0) return false;
  const hasText = selection.toString().length > 0;
  if (!hasText) return false;
  const anchor = selection.anchorNode;
  const focus = selection.focusNode;
  return (
    (anchor && target.contains(anchor)) ||
    (focus && target.contains(focus))
  );
}

function bindLogInteraction(target, setFlag) {
  if (!target) return;
  const stopInteracting = () => {
    // Allow copy selection to persist after mouseup.
    if (!isLogSelectionActive(target)) setFlag(false);
  };
  target.addEventListener("mousedown", () => {
    setFlag(true);
  });
  target.addEventListener("mouseup", () => {
    setTimeout(stopInteracting, 0);
  });
  target.addEventListener("mouseleave", stopInteracting);
  target.addEventListener(
    "touchstart",
    () => {
      setFlag(true);
    },
    { passive: true }
  );
  target.addEventListener("touchend", () => {
    setTimeout(stopInteracting, 0);
  });
}

function parseWindowTokenList(value) {
  const text = String(value || "").trim();
  if (!text) return [];
  const parts = text.split(/[、,，/\s]+/u);
  const tokens = [];
  const seen = new Set();
  parts.forEach((part) => {
    const token = String(part || "").trim();
    if (!token || seen.has(token)) return;
    seen.add(token);
    tokens.push(token);
  });
  return tokens;
}

function normalizeWindowTokenList(value) {
  const tokens = parseWindowTokenList(value);
  if (!tokens.length) return "";
  return `${tokens.join("/")}/`;
}

function isEditingDeviceWindowToken() {
  const activeElement = document.activeElement;
  if (!activeElement) return false;
  return Boolean(
    activeElement.matches?.('input[data-role="device-window-token"]') &&
      deviceTableBody?.contains(activeElement)
  );
}

function hasRecentDeviceWindowTokenEdit() {
  return Date.now() - lastDeviceWindowTokenEditAt < 2500;
}

function rememberDeviceWindowTokenDraft(machineId, value) {
  const normalizedMachineId = String(machineId || "").trim();
  if (!normalizedMachineId) return;
  deviceWindowTokenDrafts.set(normalizedMachineId, String(value || ""));
  lastDeviceWindowTokenEditAt = Date.now();
}

function forgetDeviceWindowTokenDraft(machineId) {
  const normalizedMachineId = String(machineId || "").trim();
  if (!normalizedMachineId) return;
  deviceWindowTokenDrafts.delete(normalizedMachineId);
}

bindLogInteraction(logOutput, (value) => {
  runtimeLogUserInteracting = value;
});

function renderLogOutput(target, lines, isUserInteracting) {
  if (!target) return;
  if (isUserInteracting || isLogSelectionActive(target)) return;
  target.textContent = (lines || []).join("\n");
  target.scrollTop = target.scrollHeight;
}
let userModalMode = "add";
let userModalOriginal = null;
let userModalStatus = 1;

const expiryText = document.getElementById("expiry-text");
const expiryWarning = document.getElementById("expiry-warning");

const ERROR_I18N = {
  unauthorized: "未授权，请重新登录",
  not_found: "未找到对应资源",
  invalid_status: "状态参数无效",
  missing_machine_id: "缺少设备标识",
  missing_id: "缺少必要参数",
  missing_fields: "参数不完整",
  missing_owner: "缺少账号信息",
  owner_not_found: "账号不存在",
  owner_disabled: "账号已禁用",
  account_expired: "账号已到期，请联系管理员续费",
  device_limit_reached: "设备数量已达上限",
  device_bound: "设备已绑定",
  device_disabled: "设备已禁用",
  invalid_activation_code: "授权码无效",
  missing_activation_code: "缺少授权码",
  invalid_token: "登录状态已失效，请重新登录",
  token_expired: "登录状态已过期，请重新登录",
  agent_not_registered: "设备未注册",
  missing_window_token: "缺少窗口编号",
  already_running: "任务已在运行中",
  window_running: "窗口正在运行",
  device_not_found: "设备不存在",
  user_not_found: "账号不存在",
  client_config_managed_locally: "设备配置已改为客户端本地维护",
};

function translateErrorMessage(value) {
  if (value === null || value === undefined) return "";
  const text = String(value).trim();
  if (!text) return "";
  if (/[\u4e00-\u9fa5]/.test(text)) return text;
  return ERROR_I18N[text] || text;
}

const fields = {
  templates: document.getElementById("templates"),
  popupStopTexts: document.getElementById("popup-stop-texts"),
  permanentSkipTexts: document.getElementById("permanent-skip-texts"),
  adminPopupStopTexts: document.getElementById("admin-popup-stop-texts"),
  adminPermanentSkipTexts: document.getElementById("admin-permanent-skip-texts"),
};

document.getElementById("save-btn").addEventListener("click", saveConfig);
if (startBtn) {
  startBtn.addEventListener("click", startTask);
}
if (stopBtn) {
  stopBtn.addEventListener("click", stopTask);
}
if (restartWindowBtn) {
  restartWindowBtn.addEventListener("click", restartWindow);
}
if (pauseBtn) {
  pauseBtn.addEventListener("click", pauseTask);
}
if (resumeBtn) {
  resumeBtn.addEventListener("click", resumeTask);
}
if (collectBtn) {
  collectBtn.addEventListener("click", collectGroups);
}
if (stopCollectBtn) {
  stopCollectBtn.addEventListener("click", stopCollectTask);
}
if (restartWindowToken) {
  restartWindowToken.addEventListener("input", () => applyTaskButtonState());
}
if (logoutBtn) {
  logoutBtn.addEventListener("click", handleLogout);
}
navItems.forEach((item) => {
  item.addEventListener("click", () => {
    void activatePage(item.dataset.page, item.dataset.title);
  });
});
if (addUserBtn) {
  addUserBtn.addEventListener("click", () => {
    openUserModal("add");
  });
}
if (userModalCancel) {
  userModalCancel.addEventListener("click", closeUserModal);
}
if (userModalSave) {
  userModalSave.addEventListener("click", submitUserModal);
}
if (userModalToggle) {
  userModalToggle.addEventListener("click", toggleUserStatusFromModal);
}
if (userModal) {
  userModal.addEventListener("click", (event) => {
    if (event.target.classList.contains("modal-backdrop")) {
      closeUserModal();
    }
  });
}
if (userTableBody) {
  userTableBody.addEventListener("click", handleUserTableClick);
}
if (deviceTableBody) {
  deviceTableBody.addEventListener("click", handleDeviceTableClick);
  deviceTableBody.addEventListener("input", (event) => {
    const input = event.target.closest('input[data-role="device-window-token"]');
    if (!input) return;
    const row = input.closest("tr");
    const saveButton = row ? row.querySelector('button[data-action="save-window-token"]') : null;
    const machineId = String(saveButton?.dataset.machineId || "").trim();
    if (!machineId) return;
    rememberDeviceWindowTokenDraft(machineId, input.value);
  });
}
if (deviceDetailBackBtn) {
  deviceDetailBackBtn.addEventListener("click", () => {
    void activatePage("device", "设备管理", false);
  });
}
if (deviceDetailSaveBtn) {
  deviceDetailSaveBtn.addEventListener("click", saveDeviceDetailSelections);
}
const tableSelectionRoots = [userTableBody, deviceTableBody, deviceInfoTableBody, deviceDetailTableBody].filter(Boolean);

function isTableSelectionActive() {
  const selection = window.getSelection();
  if (!selection || selection.rangeCount === 0) return false;
  if (!selection.toString().length) return false;
  const anchor = selection.anchorNode;
  const focus = selection.focusNode;
  return tableSelectionRoots.some(
    (root) =>
      (anchor && root.contains(anchor)) ||
      (focus && root.contains(focus))
  );
}

function stopTableInteracting() {
  if (!isTableSelectionActive()) {
    tableUserInteracting = false;
  }
}

tableSelectionRoots.forEach((root) => {
  root.addEventListener("mousedown", () => {
    tableUserInteracting = true;
  });
  root.addEventListener("mouseup", () => {
    setTimeout(stopTableInteracting, 0);
  });
  root.addEventListener("mouseleave", stopTableInteracting);
  root.addEventListener(
    "touchstart",
    () => {
      tableUserInteracting = true;
    },
    { passive: true }
  );
  root.addEventListener("touchend", () => {
    setTimeout(stopTableInteracting, 0);
  });
});
async function initializeApp() {
  await loadCurrentAccount();
  await loadConfig();
  const storedPage = getStoredPage();
  if (storedPage && hasPage(storedPage.key)) {
    setActivePage(storedPage.key, storedPage.title, false);
  } else {
    if (storedPage) {
      try {
        localStorage.removeItem("adminActivePage");
      } catch (error) {
        // ignore storage failures
      }
    }
    setActivePage("task", "任务管理", false);
  }
  await refreshStatus({ forceAux: true, includeWhenHidden: true });
  if (window.statusTimer) clearInterval(window.statusTimer);
  window.statusTimer = setInterval(() => {
    void refreshStatus();
  }, STATUS_POLL_INTERVAL_MS);
}

async function loadCurrentAccount() {
  if (!currentAccountName || !currentAccountRole) return;
  const response = await api("/me");
  if (!response.ok) {
    if (response.status === 401) {
      redirectToLogin();
      return;
    }
    currentAccountName.textContent = IS_SUB ? "-" : "admin";
    currentAccountRole.textContent = IS_SUB ? "子账户" : "管理员";
    applyTaskButtonState();
    return;
  }
  const account = response.data?.account || {};
  currentAccountName.textContent = account.username || (IS_SUB ? "-" : "admin");
  currentAccountRole.textContent = account.role_label || (IS_SUB ? "子账户" : "管理员");
  applyTaskButtonState();
}

async function handleLogout() {
  const confirmed = await showConfirm("确定退出当前账号吗？", "退出登录");
  if (!confirmed) return;
  const response = await api("/logout", { method: "POST" });
  if (!response.ok) {
    await showAlert(translateErrorMessage(response.data?.error) || "退出失败，请稍后重试");
    return;
  }
  try {
    localStorage.removeItem("adminActivePage");
  } catch (error) {
    // ignore storage failures
  }
  redirectToLogin();
}

function redirectToLogin() {
  window.location.href = IS_SUB ? "/sub/login" : "/admin/login";
}

async function loadConfig() {
  const response = await api("/config");
  if (!response.ok) return;

  const messages = response.data.messages || {};
  const restrictionSummary = response.data.restriction_summary || {};
  if (fields.templates) {
    fields.templates.value = (messages.templates || []).join("\n");
  }
  if (fields.popupStopTexts) {
    fields.popupStopTexts.value = (messages.popup_stop_texts || []).join("\n");
  }
  if (fields.permanentSkipTexts) {
    fields.permanentSkipTexts.value = (messages.permanent_skip_texts || []).join("\n");
  }
  if (fields.adminPopupStopTexts) {
    fields.adminPopupStopTexts.value = (restrictionSummary.popup_stop_texts || []).join("\n");
  }
  if (fields.adminPermanentSkipTexts) {
    fields.adminPermanentSkipTexts.value = (restrictionSummary.permanent_skip_texts || []).join("\n");
  }

  if (IS_SUB) {
    const account = response.data.account || {};
    updateExpiryInfo(account);
  }
}

function formatTimestamp(value) {
  if (value === null || value === undefined || value === "") return "-";
  const raw = String(value).trim();
  const num = Number(raw);
  let dt = null;
  if (Number.isFinite(num)) {
    dt = new Date(num * 1000);
  } else {
    const parsed = Date.parse(raw);
    if (!Number.isNaN(parsed)) {
      dt = new Date(parsed);
    }
  }
  if (!dt || Number.isNaN(dt.getTime())) return String(value);
  const pad = (v) => String(v).padStart(2, "0");
  const yyyy = dt.getFullYear();
  const mm = pad(dt.getMonth() + 1);
  const dd = pad(dt.getDate());
  const hh = pad(dt.getHours());
  const mi = pad(dt.getMinutes());
  return `${yyyy}/${mm}/${dd} ${hh}:${mi}`;
}

function updateExpiryInfo(account) {
  if (!expiryText) return;
  const expireAt = account?.expire_at;
  if (expireAt === null || expireAt === undefined || expireAt === "") {
    expiryText.textContent = "-";
    if (expiryWarning) {
      expiryWarning.textContent = "";
      expiryWarning.classList.add("hidden");
    }
    return;
  }
  const daysLeft = account?.days_left;
  const expiryLabel = formatTimestamp(expireAt);
  if (daysLeft === null || daysLeft === undefined) {
    expiryText.textContent = `到期时间：${expiryLabel}`;
  } else if (daysLeft < 0) {
    expiryText.textContent = "已到期";
  } else {
    expiryText.textContent = `剩余 ${daysLeft} 天`;
  }
  if (expiryWarning) {
    if (typeof daysLeft === "number" && daysLeft <= 7 && daysLeft >= 0) {
      expiryWarning.textContent = `即将到期：${expiryLabel}`;
      expiryWarning.classList.remove("hidden");
    } else {
      expiryWarning.textContent = "";
      expiryWarning.classList.add("hidden");
    }
  }
}

const alertModal = document.getElementById("alert-modal");
const alertTitle = document.getElementById("alert-title");
const alertBody = document.getElementById("alert-body");
const alertOk = document.getElementById("alert-ok");
let alertResolver = null;
const confirmModal = document.getElementById("confirm-modal");
const confirmTitle = document.getElementById("confirm-title");
const confirmBody = document.getElementById("confirm-body");
const confirmOk = document.getElementById("confirm-ok");
const confirmCancel = document.getElementById("confirm-cancel");
let confirmResolver = null;

function showAlert(message, title = "提示") {
  if (!alertModal || !alertBody || !alertTitle || !alertOk) {
    console.warn("Alert modal missing:", message);
    return Promise.resolve();
  }
  alertTitle.textContent = title;
  alertBody.textContent = message;
  alertModal.classList.remove("hidden");
  alertModal.setAttribute("aria-hidden", "false");
  return new Promise((resolve) => {
    alertResolver = resolve;
  });
}

function closeAlert() {
  if (!alertModal) return;
  alertModal.classList.add("hidden");
  alertModal.setAttribute("aria-hidden", "true");
  if (alertResolver) {
    alertResolver();
    alertResolver = null;
  }
}

if (alertOk) {
  alertOk.addEventListener("click", closeAlert);
}

function showConfirm(message, title = "确认") {
  if (!confirmModal || !confirmBody || !confirmTitle || !confirmOk || !confirmCancel) {
    console.warn("Confirm modal missing:", message);
    return Promise.resolve(false);
  }
  confirmTitle.textContent = title;
  confirmBody.textContent = message;
  confirmModal.classList.remove("hidden");
  confirmModal.setAttribute("aria-hidden", "false");
  return new Promise((resolve) => {
    confirmResolver = resolve;
  });
}

function closeConfirm(result) {
  if (!confirmModal) return;
  confirmModal.classList.add("hidden");
  confirmModal.setAttribute("aria-hidden", "true");
  if (confirmResolver) {
    confirmResolver(result);
    confirmResolver = null;
  }
}

if (confirmOk) {
  confirmOk.addEventListener("click", () => closeConfirm(true));
}
if (confirmCancel) {
  confirmCancel.addEventListener("click", () => closeConfirm(false));
}
if (confirmModal) {
  confirmModal.addEventListener("click", (event) => {
    if (event.target.classList.contains("modal-backdrop")) {
      closeConfirm(false);
    }
  });
}

async function saveConfig() {
  const response = await saveConfigSilently();
  if (response.ok) {
    showAlert("配置已保存");
  }
}

async function startTask() {
  if (!(await ensureTaskActionAllowed(startBtn))) return;
  const saved = await saveConfigSilently();
  if (!saved.ok) return;
  const response = await api("/start", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "启动失败");
    return;
  }
  await refreshStatus();
}

async function stopTask() {
  if (!(await ensureTaskActionAllowed(stopBtn))) return;
  const response = await api("/stop", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "停止失败");
    return;
  }
  await refreshStatus();
}

async function pauseTask() {
  if (!(await ensureTaskActionAllowed(pauseBtn))) return;
  const response = await api("/pause", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "暂停失败");
    return;
  }
  await refreshStatus();
}

async function resumeTask() {
  if (!(await ensureTaskActionAllowed(resumeBtn))) return;
  const response = await api("/resume", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "继续失败");
    return;
  }
  await refreshStatus();
}

async function restartWindow() {
  if (!(await ensureTaskActionAllowed(restartWindowBtn))) return;
  const windowToken = restartWindowToken.value.trim();
  if (!windowToken) {
    showAlert("请输入需要单独重启的窗口编号");
    return;
  }

  const response = await api("/restart-window", {
    method: "POST",
    body: { window_token: windowToken },
  });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "单独重启失败");
    return;
  }

  restartWindowToken.value = "";
  await refreshStatus();
}

async function collectGroups() {
  if (!(await ensureTaskActionAllowed(collectBtn))) return;
  const response = await api("/collect-groups", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "采集失败");
    return;
  }
  await refreshStatus();
}

async function stopCollectTask() {
  if (!(await ensureTaskActionAllowed(stopCollectBtn))) return;
  const response = await api("/stop-collect", { method: "POST", body: {} });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "停止采集失败");
    return;
  }
  await refreshStatus();
}

function ensureDashboardLabels() {
  if (dashboardLabelsReady) return;

  const setStatTitle = (valueEl, titleText) => {
    if (!valueEl) return;
    const card = valueEl.closest(".stat-card");
    if (!card) return;
    const title = card.querySelector(".stat-title");
    if (title) title.textContent = titleText;
  };

  setStatTitle(
    dashActiveToday,
    IS_SUB ? "成功触达用户数" : "成功触达账号数"
  );
  setStatTitle(
    dashUserToday,
    IS_SUB ? "在线设备数" : "今日新增子账户"
  );

  const legend = document.querySelector(".chart-legend");
  if (legend) {
    const updateLegendLabel = (key, text) => {
      const dot = legend.querySelector(`.dot.${key}`);
      if (!dot) return;
      let node = dot.nextSibling;
      while (node && node.nodeType === Node.TEXT_NODE && !node.textContent.trim()) {
        node = node.nextSibling;
      }
      if (node && node.nodeType === Node.TEXT_NODE) {
        node.textContent = text;
      } else {
        dot.insertAdjacentText("afterend", text);
      }
    };
    updateLegendLabel("user", IS_SUB ? "触达用户" : "触达账号");
    updateLegendLabel("active", "成功触达");
  }

  dashboardLabelsReady = true;
}

function isAuxRefreshDue(lastRefreshedAt, force = false) {
  if (force) return true;
  return Date.now() - Number(lastRefreshedAt || 0) >= AUX_REFRESH_INTERVAL_MS;
}

function shouldRefreshUsers(force = false) {
  return !IS_SUB && currentPageKey === "user" && isAuxRefreshDue(lastUsersRefreshAt, force);
}

function shouldRefreshDevices(force = false) {
  if (IS_SUB) {
    return ["task", "device"].includes(currentPageKey) && isAuxRefreshDue(lastDevicesRefreshAt, force);
  }
  return currentPageKey === "device" && isAuxRefreshDue(lastDevicesRefreshAt, force);
}

function shouldRefreshDeviceInfo(force = false) {
  if (IS_SUB) {
    return ["task", "device-info"].includes(currentPageKey) && isAuxRefreshDue(lastDeviceInfoRefreshAt, force);
  }
  return currentPageKey === "device-info" && isAuxRefreshDue(lastDeviceInfoRefreshAt, force);
}

async function refreshAuxiliaryData({ force = false } = {}) {
  const jobs = [];
  if (shouldRefreshUsers(force)) {
    jobs.push(refreshUsers());
  }
  if (shouldRefreshDevices(force)) {
    jobs.push(refreshDevices());
  }
  if (shouldRefreshDeviceInfo(force)) {
    jobs.push(refreshDeviceInfo());
  }
  if (!jobs.length) {
    return;
  }
  await Promise.all(jobs);
}

async function refreshStatus({ forceAux = false, includeWhenHidden = false } = {}) {
  if (!includeWhenHidden && document.visibilityState === "hidden") {
    return;
  }
  const response = await api("/status");
  if (!response.ok) {
    latestStatusPayload = null;
    applyTaskButtonState();
    return;
  }

  const status = response.data;
  latestStatusPayload = status;
  const dbStats = status.stats?.db || {};
  const runtimeStats = status.stats?.runtime || {};
  ensureDashboardLabels();
  const runningWindowCount = Number(runtimeStats.running_windows ?? 0);

  if (dashSendToday) dashSendToday.textContent = String(dbStats.sent_today ?? 0);
  if (dashSendYesterday) dashSendYesterday.textContent = `昨日 ${dbStats.sent_yesterday ?? 0}`;
  if (dashSendTotal) dashSendTotal.textContent = `总计 ${dbStats.sent_total ?? 0}`;
  if (dashFailToday) dashFailToday.textContent = String(dbStats.failed_today ?? 0);
  if (dashFailYesterday) dashFailYesterday.textContent = `昨日 ${dbStats.failed_yesterday ?? 0}`;
  if (dashFailTotal) dashFailTotal.textContent = `总计 ${dbStats.failed_total ?? 0}`;
  if (IS_SUB) {
    const onlineDevices = Number(dbStats.device_online ?? 0);
    const maxDevices = dbStats.max_devices;
    const maxLabel =
      maxDevices === null || maxDevices === undefined || maxDevices === ""
        ? "-"
        : String(maxDevices);
    if (dashUserToday) dashUserToday.textContent = `${onlineDevices} / ${maxLabel}`;
    if (dashUserYesterday)
      dashUserYesterday.textContent = `昨日在线 ${dbStats.device_yesterday ?? 0}`;
    if (dashUserTotal)
      dashUserTotal.textContent = `绑定总数 ${dbStats.device_total ?? 0}`;
  } else {
    if (dashUserToday) dashUserToday.textContent = String(dbStats.user_today ?? 0);
    if (dashUserYesterday)
      dashUserYesterday.textContent = `昨日新增 ${dbStats.user_yesterday ?? 0}`;
    if (dashUserTotal) dashUserTotal.textContent = `总计 ${dbStats.user_total ?? 0}`;
  }
  if (dashActiveToday) dashActiveToday.textContent = String(dbStats.active_today ?? 0);
  if (dashActiveYesterday)
    dashActiveYesterday.textContent = `昨日成功触达 ${dbStats.active_yesterday ?? 0}`;
  if (dashActiveTotal)
    dashActiveTotal.textContent = `成功触达总计 ${dbStats.active_total ?? 0}`;
  if (logActiveCount)
    logActiveCount.textContent = String(runtimeStats.running_accounts ?? 0);
  if (logWindowCount) logWindowCount.textContent = String(runningWindowCount);
  if (logSentCount) logSentCount.textContent = String(dbStats.sent_today ?? 0);
  if (logFailedCount) logFailedCount.textContent = String(dbStats.failed_today ?? 0);
  if (dbStats.trend) {
    lastTrend = dbStats.trend;
    renderTrendAxis(dbStats.trend.labels);
    renderTrendChart(dbStats.trend);
  }

  const mergedLogs = [
    ...(status.control_logs || []),
    ...(status.logs || []),
  ];
  renderLogOutput(logOutput, mergedLogs, runtimeLogUserInteracting);

  await refreshAuxiliaryData({ force: forceAux });
  applyTaskButtonState();
}

async function activatePage(pageKey, title, persist = true) {
  setActivePage(pageKey, title, persist);
  await refreshStatus({ forceAux: true, includeWhenHidden: true });
}

async function saveConfigSilently() {
  const messages = {};
  if (currentPageKey === "template" && fields.templates) {
    messages.templates = splitLines(fields.templates.value || "");
    if (!messages.templates.length) {
      showAlert("正式打招呼文案不能为空");
      return { ok: false };
    }
  } else if (IS_SUB && currentPageKey === "restriction") {
    messages.popup_stop_texts = splitLines(
      fields.popupStopTexts ? fields.popupStopTexts.value : ""
    );
    messages.permanent_skip_texts = splitLines(
      fields.permanentSkipTexts ? fields.permanentSkipTexts.value : ""
    );
  } else {
    return { ok: true, skipped: true };
  }

  const response = await api("/config", {
    method: "POST",
    body: { messages },
  });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "保存失败");
    return response;
  }
  return response;
}

function splitLines(value) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function setActivePage(pageKey, title, persist = true) {
  currentPageKey = pageKey;
  const activeNavPage = pageKey === "device-detail" ? "device" : pageKey;
  navItems.forEach((item) => item.classList.toggle("active", item.dataset.page === activeNavPage));
  pageCards.forEach((card) => card.classList.toggle("active", card.dataset.page === pageKey));
  if (pageTitle && title) {
    pageTitle.textContent = title;
  }
  if (saveBtn) {
    const editable = pageKey === "template" || (IS_SUB && pageKey === "restriction");
    saveBtn.classList.toggle("hidden", !editable);
  }
  if (persist) {
    try {
      localStorage.setItem(
        "adminActivePage",
        JSON.stringify({ key: pageKey, title })
      );
    } catch (error) {
      // ignore storage failures
    }
  }
  if (pageKey === "dashboard" && lastTrend) {
    requestAnimationFrame(() => {
      renderTrendAxis(lastTrend.labels);
      renderTrendChart(lastTrend);
    });
  }
}

function getStoredPage() {
  try {
    const raw = localStorage.getItem("adminActivePage");
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (parsed && parsed.key) {
      return parsed;
    }
  } catch (error) {
    return null;
  }
  return null;
}

function hasPage(pageKey) {
  if (!pageKey) return false;
  const hasNav = navItems.some((item) => item.dataset.page === pageKey);
  const hasCard = pageCards.some((card) => card.dataset.page === pageKey);
  return hasNav && hasCard;
}

function renderTrendAxis(labels) {
  if (!trendAxis) return;
  const items = Array.from(trendAxis.querySelectorAll("span"));
  if (!items.length) return;
  const normalized = Array.isArray(labels) ? labels : [];
  items.forEach((item, index) => {
    item.textContent = normalized[index] || "";
  });
}

function renderTrendChart(trend) {
  if (!trendChart) return;
  const series = {
    send: Array.isArray(trend.send) ? trend.send : [],
    fail: Array.isArray(trend.fail) ? trend.fail : [],
    user: Array.isArray(trend.user) ? trend.user : [],
    active: Array.isArray(trend.active) ? trend.active : [],
  };
  const length = Math.max(
    series.send.length,
    series.fail.length,
    series.user.length,
    series.active.length
  );
  if (!length) {
    trendChart.innerHTML = "";
    return;
  }

  const rect = trendChart.getBoundingClientRect();
  let width = Math.max(1, Math.round(rect.width || trendChart.clientWidth));
  let height = Math.max(1, Math.round(rect.height || trendChart.clientHeight));
  if ((!width || !height) && trendChart.parentElement) {
    const parentRect = trendChart.parentElement.getBoundingClientRect();
    width = Math.max(1, Math.round(parentRect.width - 32));
    height = Math.max(1, Math.round(parentRect.height - 32));
  }
  if (!width || !height) return;
  const values = []
    .concat(series.send, series.fail, series.user, series.active)
    .map((value) => Number(value) || 0);
  const maxValue = Math.max(1, ...values);

  const toPoints = (data) =>
    data.map((value, index) => {
      const x = length === 1 ? width / 2 : (index / (length - 1)) * width;
      const y = height - (Math.max(0, Number(value) || 0) / maxValue) * height;
      return { x, y };
    });

  const buildSmoothPath = (points) => {
    if (!points.length) return "";
    if (points.length === 1) {
      const p = points[0];
      return `M${p.x.toFixed(1)},${p.y.toFixed(1)}`;
    }
    let d = `M${points[0].x.toFixed(1)},${points[0].y.toFixed(1)}`;
    for (let i = 0; i < points.length - 1; i += 1) {
      const p0 = points[i - 1] || points[i];
      const p1 = points[i];
      const p2 = points[i + 1];
      const p3 = points[i + 2] || p2;
      const c1x = p1.x + (p2.x - p0.x) / 6;
      const c1y = p1.y + (p2.y - p0.y) / 6;
      const c2x = p2.x - (p3.x - p1.x) / 6;
      const c2y = p2.y - (p3.y - p1.y) / 6;
      d += ` C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(
        1
      )} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
    }
    return d;
  };

  const paths = [
    { key: "send", color: "#5b7cfa" },
    { key: "fail", color: "#f25f5c" },
    { key: "user", color: "#38b2ac" },
    { key: "active", color: "#f6ad55" },
  ]
    .map((item) => {
      const data = series[item.key] || [];
      const points = toPoints(data);
      const linePath = buildSmoothPath(points);
      if (!linePath) return "";
      const last = points[points.length - 1];
      const first = points[0];
      const areaPath = `${linePath} L${last.x.toFixed(1)},${height.toFixed(
        1
      )} L${first.x.toFixed(1)},${height.toFixed(1)} Z`;
      return [
        `<path d="${areaPath}" fill="${item.color}" opacity="0.12" stroke="none" />`,
        `<path d="${linePath}" fill="none" stroke="${item.color}" stroke-width="2" />`,
      ].join("");
    })
    .join("");

  trendChart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  trendChart.innerHTML = paths;
}

async function refreshUsers() {
  if (!userTableBody || IS_SUB) {
    return;
  }
  if (tableUserInteracting || isTableSelectionActive()) {
    return;
  }
  const response = await api("/users");
  if (!response.ok) return;
  const payload = response.data || {};
  lastUsersRefreshAt = Date.now();
  renderUserTable(payload.users || []);
}

async function refreshDevices() {
  if (!deviceTableBody) {
    return;
  }
  if (
    tableUserInteracting ||
    isTableSelectionActive() ||
    isEditingDeviceWindowToken() ||
    hasRecentDeviceWindowTokenEdit()
  ) {
    return;
  }
  const response = await api("/agent/list");
  if (!response.ok) return;
  const payload = response.data || {};
  lastDevicesRefreshAt = Date.now();
  latestAgentRows = Array.isArray(payload.agents) ? payload.agents : [];
  renderDeviceTable(latestAgentRows);
  applyTaskButtonState();
}

async function refreshDeviceInfo() {
  if (!deviceInfoTableBody) return;
  if (tableUserInteracting || isTableSelectionActive()) {
    return;
  }
  const response = await api("/device-info");
  if (!response.ok) return;
  const payload = response.data || {};
  lastDeviceInfoRefreshAt = Date.now();
  latestDeviceInfoRows = Array.isArray(payload.configs) ? payload.configs : [];
  renderDeviceInfoTable(latestDeviceInfoRows);
  applyTaskButtonState();
}

function setButtonDisabled(button, disabled, reason = "") {
  if (!button) return;
  button.disabled = !!disabled;
  if (reason) {
    button.title = reason;
  } else {
    button.removeAttribute("title");
  }
}

function setTaskActionButtonState(button, blocked, reason = "") {
  if (!button) return;
  const normalizedReason = String(reason || "").trim();
  button.disabled = false;
  button.dataset.blocked = blocked ? "1" : "0";
  button.dataset.blockReason = blocked ? normalizedReason : "";
  button.classList.toggle("is-blocked", !!blocked);
  button.setAttribute("aria-disabled", blocked ? "true" : "false");
  if (normalizedReason) {
    button.title = normalizedReason;
  } else {
    button.removeAttribute("title");
  }
}

async function ensureTaskActionAllowed(button) {
  if (!button) return false;
  if (button.dataset.blocked !== "1") return true;
  const reason = String(button.dataset.blockReason || "").trim() || "当前操作暂不可执行";
  await showAlert(reason);
  return false;
}

function applyTaskButtonState() {
  if (!IS_SUB) {
    return;
  }
  const status = latestStatusPayload;
  if (!status) {
    setTaskActionButtonState(startBtn, true, "状态同步中");
    setTaskActionButtonState(stopBtn, true, "状态同步中");
    setTaskActionButtonState(pauseBtn, true, "状态同步中");
    setTaskActionButtonState(resumeBtn, true, "状态同步中");
    setTaskActionButtonState(collectBtn, true, "状态同步中");
    setTaskActionButtonState(stopCollectBtn, true, "状态同步中");
    setTaskActionButtonState(restartWindowBtn, true, "状态同步中");
    return;
  }

  const relevantAgents = latestAgentRows;
  const relevantConfigs = latestDeviceInfoRows;
  const enabledConfigs = relevantConfigs.filter((item) => Number(item.status ?? 1) !== 0);
  const hasBoundDevice = enabledConfigs.some((item) => String(item.machine_id || "").trim());
  const hasConnectionFields = enabledConfigs.some(
    (item) => String(item.bit_api || "").trim() && String(item.api_token || "").trim()
  );
  const agentOnline = relevantAgents.some(
    (item) => Number(item.status ?? 1) !== 0 && Boolean(item.online)
  );
  const taskRunning = Boolean(status.task_running);
  const taskPending = Boolean(status.task_pending);
  const taskPaused = Boolean(status.task_paused);
  const collectRunning = Boolean(status.collect_running);
  const collectPending = Boolean(status.collect_pending);
  const startReady = agentOnline && hasBoundDevice && hasConnectionFields;
  let startBlockReason = "";
  if (!hasBoundDevice) {
    startBlockReason = "未绑定执行端";
  } else if (!hasConnectionFields) {
    startBlockReason = "设备配置缺少接口参数";
  } else if (!agentOnline) {
    startBlockReason = "执行端离线";
  }

  const isBusy = taskRunning || taskPending || taskPaused;
  const collectBusy = collectRunning || collectPending;
  const startReason =
    startBlockReason ||
    (collectRunning
      ? "采集进行中"
      : collectPending
        ? "采集排队中"
        : taskPaused
          ? "任务暂停中"
          : taskRunning
            ? "任务运行中"
            : taskPending
              ? "任务排队中"
              : "");
  setTaskActionButtonState(startBtn, !startReady || isBusy || collectBusy, startReason);
  setTaskActionButtonState(stopBtn, !isBusy, isBusy ? "" : "当前没有运行中的任务");
  setTaskActionButtonState(
    pauseBtn,
    !taskRunning || taskPaused,
    taskPaused ? "任务已暂停" : (taskRunning ? "" : "当前没有运行中的任务")
  );
  setTaskActionButtonState(
    resumeBtn,
    !taskPaused,
    taskPaused ? "" : (taskRunning ? "任务未暂停" : "当前没有运行中的任务")
  );
  const collectReason =
    startBlockReason ||
    (taskPaused
      ? "任务暂停中"
      : taskRunning
        ? "任务运行中"
        : taskPending
          ? "任务排队中"
          : collectRunning
            ? "采集进行中"
            : collectPending
              ? "采集排队中"
              : "");
  setTaskActionButtonState(collectBtn, !startReady || isBusy || collectBusy, collectReason);
  setTaskActionButtonState(
    stopCollectBtn,
    !collectBusy,
    collectBusy ? "" : "当前没有采集中的任务"
  );
  const windowToken = String(restartWindowToken?.value || "").trim();
  const restartReason = !windowToken ? "请输入窗口编号" : startReason;
  setTaskActionButtonState(restartWindowBtn, !windowToken || !startReady || collectBusy, restartReason);
}

function renderDeviceInfoTable(configs) {
  if (!deviceInfoTableBody) return;
  const rows = Array.isArray(configs) ? configs : [];
  const isAdminView = !IS_SUB;
  rows.sort((a, b) => {
    const left = `${a.owner || ""}\u0000${a.name || ""}\u0000${a.machine_id || ""}`;
    const right = `${b.owner || ""}\u0000${b.name || ""}\u0000${b.machine_id || ""}`;
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
  });
  if (!rows.length) {
    deviceInfoTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="${isAdminView ? 8 : 7}">暂无数据</td>
      </tr>
    `;
    return;
  }
  deviceInfoTableBody.innerHTML = rows
    .map((item, index) => {
      const status = Number(item.status ?? 1);
      const isDisabled = status === 0;
      const displayName = item.name || item.machine_id || "-";
      const onlineText = item.online ? "在线" : "离线";
      const lastSeenText = item.last_seen ? formatTimestamp(item.last_seen) : "-";
      if (isAdminView) {
        const apiTokenText = item.api_token || "-";
        return `
          <tr class="${isDisabled ? "is-disabled" : ""}">
            <td class="col-index">${index + 1}</td>
            <td class="copy-cell">${renderCopyCell(item.owner || "-", { mono: false })}</td>
            <td class="copy-cell">${renderCopyCell(displayName, { mono: false })}</td>
            <td class="copy-cell">${renderCopyCell(item.machine_id || "-")}</td>
            <td class="copy-cell">${renderCopyCell(item.bit_api || "-")}</td>
            <td class="copy-cell">${renderCopyCell(apiTokenText)}</td>
            <td>${onlineText}</td>
            <td>${escapeHtml(lastSeenText)}</td>
          </tr>
        `;
      }
      const apiTokenText = item.api_token || "-";
      return `
        <tr class="${isDisabled ? "is-disabled" : ""}">
          <td class="col-index">${index + 1}</td>
          <td class="copy-cell">${renderCopyCell(displayName, { mono: false })}</td>
          <td class="copy-cell">${renderCopyCell(item.machine_id || "-")}</td>
          <td class="copy-cell">${renderCopyCell(item.bit_api || "-")}</td>
          <td class="copy-cell">${renderCopyCell(apiTokenText)}</td>
          <td>${onlineText || "离线"}</td>
          <td>${escapeHtml(lastSeenText || "-")}</td>
        </tr>
      `;
    })
    .join("");
}

function renderDeviceTable(agents) {
  if (!deviceTableBody) return;
  const rows = Array.isArray(agents) ? agents : [];
  rows.sort((a, b) => {
    const left = String(a.machine_id || "");
    const right = String(b.machine_id || "");
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
  });
  if (!rows.length) {
    deviceTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="${IS_SUB ? 8 : 8}">暂无数据</td>
      </tr>
    `;
    return;
  }
  deviceTableBody.innerHTML = rows
    .map((agent, index) => {
      const lastSeen = Number(agent.last_seen || 0);
      const lastSeenText = lastSeen ? formatTimestamp(lastSeen) : "-";
      const onlineText = agent.online ? "在线" : "离线";
      const status = Number(agent.status ?? 1);
      const isDisabled = status === 0;
      const statusText = isDisabled ? "禁用" : "启用";
      const toggleLabel = isDisabled ? "启用" : "禁用";
      const machineId = String(agent.machine_id || "").trim();
      const draftWindowToken = deviceWindowTokenDrafts.get(machineId);
      const preferredWindowToken =
        draftWindowToken !== undefined
          ? String(draftWindowToken)
          : normalizeWindowTokenList(agent.preferred_window_token || "");
      const detailButton = `
            <button
              class="action-btn"
              data-action="device-detail"
              data-machine-id="${escapeHtml(agent.machine_id || "")}"
              data-owner="${escapeHtml(agent.owner || "")}"
            >详情</button>
          `;
      const unbindButton = `
            <button
              class="action-btn danger"
              data-action="unbind-device"
              data-machine-id="${escapeHtml(agent.machine_id || "")}"
            >解绑</button>
          `;
      const saveWindowButton = `
            <button
              class="action-btn"
              data-action="save-window-token"
              data-machine-id="${escapeHtml(agent.machine_id || "")}"
            >保存</button>
          `;
      if (IS_SUB) {
        return `
          <tr class="${isDisabled ? "is-disabled" : ""}">
            <td class="col-index">${index + 1}</td>
            <td class="copy-cell">${renderCopyCell(agent.machine_id || "-")}</td>
            <td class="copy-cell">${renderCopyCell(agent.owner || "-", { mono: false })}</td>
            <td>${onlineText}</td>
            <td>${escapeHtml(lastSeenText)}</td>
            <td class="col-window-token">
              <input
                type="text"
                class="mini-inline-input"
                data-role="device-window-token"
                value="${escapeHtml(preferredWindowToken)}"
                placeholder="默认全部 / 例 1/3/5/"
              />
            </td>
            <td class="col-device-status">${statusText}</td>
            <td class="col-actions">
              ${detailButton}
              ${saveWindowButton}
              <button
                class="action-btn ${isDisabled ? "" : "danger"}"
                data-action="toggle-device"
                data-machine-id="${escapeHtml(agent.machine_id || "")}"
                data-disabled="${isDisabled ? "1" : "0"}"
              >${toggleLabel}</button>
              ${unbindButton}
            </td>
          </tr>
        `;
      }
      return `
        <tr class="${isDisabled ? "is-disabled" : ""}">
          <td class="col-index">${index + 1}</td>
          <td class="copy-cell">${renderCopyCell(agent.machine_id || "-")}</td>
          <td class="copy-cell">${renderCopyCell(agent.owner || "-", { mono: false })}</td>
          <td>${onlineText}</td>
          <td>${escapeHtml(lastSeenText)}</td>
          <td>${escapeHtml(agent.client_version || "-")}</td>
          <td>${statusText}</td>
          <td class="col-actions">
            ${detailButton}
            <button
              class="action-btn ${isDisabled ? "" : "danger"}"
              data-action="toggle-device"
              data-machine-id="${escapeHtml(agent.machine_id || "")}"
              data-disabled="${isDisabled ? "1" : "0"}"
            >${toggleLabel}</button>
            ${unbindButton}
          </td>
        </tr>
      `;
    })
    .join("");
}

function renderUserTable(users) {
  if (!userTableBody) return;
  const rows = Array.isArray(users) ? users : [];
  if (!rows.length) {
    userTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="8">暂无数据</td>
      </tr>
    `;
    return;
  }
  userTableBody.innerHTML = rows
    .map((user, index) => {
      const createdAt = user.created_at || "-";
      const createdAtText = formatTimestamp(createdAt);
      const expireAt = user.expire_at ?? "";
      const expireAtText = formatTimestamp(expireAt);
      const daysLeftRaw = user.days_left;
      let daysLeftText = "-";
      if (expireAt) {
        if (daysLeftRaw === null || daysLeftRaw === undefined || daysLeftRaw === "") {
          daysLeftText = "-";
        } else if (Number(daysLeftRaw) <= 0) {
          daysLeftText = "已到期";
        } else {
          daysLeftText = String(daysLeftRaw);
        }
      }
      const status = Number(user.status ?? 1);
      const isDisabled = status === 0;
      const rowClass = isDisabled ? "is-disabled" : "";
      const activationCode = user.activation_code || "-";
      return `
        <tr class="${rowClass}">
          <td class="col-index">${index + 1}</td>
          <td class="copy-cell">${renderCopyCell(user.username || "-", { mono: false })}</td>
          <td>
            <div class="activation-cell copy-cell">
              ${renderCopyCell(activationCode)}
            </div>
          </td>
          <td>${escapeHtml(user.max_devices ?? "-")}</td>
          <td>${escapeHtml(expireAtText)}</td>
          <td>${escapeHtml(daysLeftText)}</td>
          <td>${escapeHtml(createdAtText)}</td>
          <td class="col-actions">
            <button
              class="action-btn"
              data-action="edit"
              data-username="${escapeHtml(user.username || "")}"
              data-max-devices="${escapeHtml(user.max_devices ?? "")}"
              data-expire-at="${escapeHtml(expireAt)}"
              data-status="${status}"
            >编辑</button>
            <button
              class="action-btn danger"
              data-action="delete"
              data-username="${escapeHtml(user.username || "")}"
            >删除</button>
          </td>
        </tr>
      `;
    })
    .join("");
}

function openUserModal(mode, user = {}) {
  if (!userModal) return;
  userModalMode = mode;
  userModalOriginal = user.username || null;
  userModalStatus = Number(user.status ?? 1);
  if (userModalTitle) {
    userModalTitle.textContent = mode === "edit" ? "编辑用户" : "添加用户";
  }
  if (userModalToggle) {
    if (mode === "edit") {
      userModalToggle.textContent = userModalStatus === 0 ? "启用" : "禁用";
      userModalToggle.dataset.disabled = userModalStatus === 0 ? "1" : "0";
      userModalToggle.classList.remove("hidden");
    } else {
      userModalToggle.classList.add("hidden");
    }
  }
  if (userModalUsername) {
    userModalUsername.value = user.username || "";
  }
  if (userModalPassword) {
    userModalPassword.value = "";
    userModalPassword.placeholder = mode === "edit" ? "留空则不修改" : "请输入密码";
  }
  if (userModalMaxDevices) {
    userModalMaxDevices.value = user.max_devices ?? "";
  }
  if (userModalPlan) {
    userModalPlan.value = mode === "edit" ? "" : "1";
  }
  if (userModalExpireDisplay) {
    const expireText = formatTimestamp(user.expire_at ?? "");
    userModalExpireDisplay.value = expireText || "-";
  }
  if (userModalError) {
    userModalError.textContent = "";
    userModalError.classList.add("hidden");
  }
  userModal.classList.remove("hidden");
}

function closeUserModal() {
  if (!userModal) return;
  userModal.classList.add("hidden");
}

async function toggleUserStatusFromModal() {
  if (!userModalToggle) return;
  const username = userModalOriginal || (userModalUsername ? userModalUsername.value.trim() : "");
  if (!username) return;
  const disabled = userModalToggle.dataset.disabled === "1";
  const confirmText = disabled ? "确定要启用该账号吗？" : "确定要禁用该账号吗？";
  if (!(await showConfirm(confirmText))) return;
  const response = await api("/users/disable", {
    method: "POST",
    body: { username, disabled: !disabled },
  });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "操作失败");
    return;
  }
  userModalStatus = disabled ? 1 : 0;
  userModalToggle.textContent = userModalStatus === 0 ? "启用" : "禁用";
  userModalToggle.dataset.disabled = userModalStatus === 0 ? "1" : "0";
  await refreshUsers();
}


async function submitUserModal() {
  if (!userModal) return;
  const payload = {
    username: userModalUsername ? userModalUsername.value.trim() : "",
    password: userModalPassword ? userModalPassword.value.trim() : "",
    max_devices: userModalMaxDevices ? userModalMaxDevices.value.trim() : "",
  };
  const planMonths = userModalPlan ? userModalPlan.value.trim() : "";
  if (userModalMode === "add" && !planMonths) {
    if (userModalError) {
      userModalError.textContent = "请选择租期";
      userModalError.classList.remove("hidden");
    }
    return;
  }
  if (planMonths) {
    payload.plan_months = planMonths;
  }
  let endpoint = "/users/add";
  if (userModalMode === "edit") {
    endpoint = "/users/update";
    payload.original_username = userModalOriginal;
  }
  const response = await api(endpoint, { method: "POST", body: payload });
  if (!response.ok) {
    if (userModalError) {
      userModalError.textContent = translateErrorMessage(response.data?.error) || "操作失败";
      userModalError.classList.remove("hidden");
    }
    return;
  }
  closeUserModal();
  await refreshUsers();
}

async function handleUserTableClick(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  const username = button.dataset.username || "";
  if (!username) return;

  if (action === "edit") {
    openUserModal("edit", {
      username,
      max_devices: button.dataset.maxDevices || "",
      expire_at: button.dataset.expireAt || "",
      status: Number(button.dataset.status ?? 1),
    });
    return;
  }

  if (action === "delete") {
    if (!(await showConfirm("确定要删除该账号吗？"))) return;
    const response = await api("/users/delete", {
      method: "POST",
      body: { username },
    });
    if (!response.ok) {
      showAlert(translateErrorMessage(response.data?.error) || "操作失败");
      return;
    }
    await refreshUsers();
    return;
  }
}

async function handleDeviceTableClick(event) {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const action = button.dataset.action;
  const machineId = button.dataset.machineId || "";
  if (!machineId) return;
  if (action === "save-window-token") {
    const row = button.closest("tr");
    const input = row ? row.querySelector('input[data-role="device-window-token"]') : null;
    const preferredWindowToken = normalizeWindowTokenList(input ? input.value : "");
    if (input) {
      input.value = preferredWindowToken;
    }
    rememberDeviceWindowTokenDraft(machineId, preferredWindowToken);
    const response = await api("/agent/window-token", {
      method: "POST",
      body: { machine_id: machineId, preferred_window_token: preferredWindowToken },
    });
    if (!response.ok) {
      showAlert(translateErrorMessage(response.data?.error) || "操作失败");
      return;
    }
    forgetDeviceWindowTokenDraft(machineId);
    await refreshDevices();
    return;
  }
  if (action === "device-detail") {
    const owner = button.dataset.owner || "";
    await loadDeviceDetail(machineId, owner);
    return;
  }
  if (action !== "toggle-device" && action !== "unbind-device") return;
  if (action === "unbind-device") {
    if (!(await showConfirm("确定要解绑该设备吗？解绑后该设备将立即离线并释放配额。"))) {
      return;
    }
    const response = await api("/agent/unbind", {
      method: "POST",
      body: { machine_id: machineId },
    });
    if (!response.ok) {
      showAlert(translateErrorMessage(response.data?.error) || "操作失败");
      return;
    }
    await refreshDevices();
    return;
  }
  const disabled = button.dataset.disabled === "1";
  const confirmText = disabled ? "确定要启用该设备吗？" : "确定要禁用该设备吗？";
  if (!(await showConfirm(confirmText))) return;
  const response = await api("/agent/disable", {
    method: "POST",
    body: { machine_id: machineId, status: disabled ? 1 : 0 },
  });
  if (!response.ok) {
    showAlert(translateErrorMessage(response.data?.error) || "操作失败");
    return;
  }
  await refreshDevices();
}

function setDeviceDetailEmpty(message = "请先从设备管理进入详情页") {
  if (deviceDetailMeta) {
    deviceDetailMeta.textContent = message;
  }
  if (deviceDetailTableBody) {
    deviceDetailTableBody.innerHTML = `
      <tr class="empty-row">
        <td colspan="7">${escapeHtml(message)}</td>
      </tr>
    `;
  }
  setButtonDisabled(deviceDetailSaveBtn, true, message);
}

function normalizeCollectedGroupName(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  return text
    .replace(/\s*上次发帖[:：].*$/u, "")
    .replace(/\s*最近发帖[:：].*$/u, "")
    .replace(/\s*Last post[:：].*$/iu, "")
    .trim();
}

function isGenericCollectedGroupName(value) {
  const normalized = String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
  return !!normalized && GROUP_ACTION_LABELS.has(normalized);
}

function deriveCollectedGroupNameFromUrl(groupUrl, groupId = "") {
  const normalizedUrl = String(groupUrl || "").trim();
  const normalizedGroupId = String(groupId || "").trim();
  if (!normalizedUrl && !normalizedGroupId) return "";
  let slug = "";
  if (normalizedUrl) {
    try {
      const url = new URL(normalizedUrl);
      const parts = url.pathname.split("/").filter(Boolean);
      if (parts.length >= 2 && parts[0] === "groups") {
        slug = decodeURIComponent(parts[1] || "");
      }
    } catch (_error) {
      slug = "";
    }
  }
  slug = String(slug || "").trim();
  if (slug) {
    if (/^\d+$/.test(slug)) {
      return `群组 ${slug}`;
    }
    return slug.replace(/[-_]+/g, " ").trim();
  }
  if (normalizedGroupId) {
    return /^\d+$/.test(normalizedGroupId) ? `群组 ${normalizedGroupId}` : normalizedGroupId;
  }
  return "";
}

function resolveCollectedGroupName(group) {
  const normalizedName = normalizeCollectedGroupName(group?.group_name);
  if (normalizedName && !isGenericCollectedGroupName(normalizedName)) {
    return normalizedName;
  }
  return deriveCollectedGroupNameFromUrl(group?.group_url, group?.group_id) || normalizedName;
}

function resolveWindowCollectStatus(windowItem) {
  const status = String(windowItem?.last_collect_status || "").trim();
  const message = String(windowItem?.last_collect_message || "").trim();
  if (message) return message;
  if (status === "facebook_not_logged_in") {
    return "未登录 Facebook，当前窗口采集已跳过";
  }
  if (status === "account_restricted") {
    return "命中 Facebook checkpoint/风控页，当前窗口采集已跳过";
  }
  if (status === "session_open_failed") {
    return "连接浏览器失败，当前窗口采集未执行";
  }
  if (status) {
    return `采集失败：${status}`;
  }
  return "当前窗口暂无群组采集结果";
}

function getDevicePreferredWindowToken(owner, machineId) {
  const normalizedOwner = String(owner || "").trim();
  const normalizedMachineId = String(machineId || "").trim();
  if (!normalizedMachineId) return "";
  const matched = latestAgentRows.find((item) => {
    const itemMachineId = String(item?.machine_id || "").trim();
    const itemOwner = String(item?.owner || "").trim();
    if (!itemMachineId || itemMachineId !== normalizedMachineId) return false;
    if (!normalizedOwner) return true;
    return itemOwner === normalizedOwner;
  });
  return normalizeWindowTokenList(matched?.preferred_window_token || "");
}

function renderDeviceDetail(detail) {
  currentDeviceDetail = detail || null;
  const device = detail?.device || {};
  const allWindows = Array.isArray(detail?.windows) ? detail.windows : [];
  const owner = String(device.owner || detail?.owner || "").trim();
  const machineId = String(device.machine_id || detail?.machine_id || "").trim();
  const preferredWindowTokenText =
    normalizeWindowTokenList(device.preferred_window_token || "") ||
    getDevicePreferredWindowToken(owner, machineId);
  const preferredWindowTokens = parseWindowTokenList(preferredWindowTokenText);
  const preferredWindowTokenSet = new Set(preferredWindowTokens);
  const windows = preferredWindowTokens.length
    ? allWindows.filter((windowItem) =>
        preferredWindowTokenSet.has(String(windowItem?.window_token || "").trim())
      )
    : allWindows;
  currentDeviceDetailVisibleWindowTokens = windows
    .map((windowItem) => String(windowItem?.window_token || "").trim())
    .filter(Boolean);
  if (deviceDetailMeta) {
    const parts = [
      `归属账号：${owner || "-"}`,
      `设备ID：${machineId || "-"}`,
      `配置名称：${device.name || "-"}`,
      `窗口号：${preferredWindowTokenText || "全部"}`,
      `在线状态：${device.online ? "在线" : "离线"}`,
      `最后心跳：${device.last_seen ? formatTimestamp(device.last_seen) : "-"}`,
    ];
    deviceDetailMeta.textContent = parts.join("    ");
  }
  if (!deviceDetailTableBody) return;
  const rows = [];
  let sequence = 0;
  windows.forEach((windowItem) => {
    const windowToken = String(windowItem.window_token || "").trim();
    const windowName = String(windowItem.window_name || "").trim() || "-";
    const groups = Array.isArray(windowItem.groups) ? windowItem.groups : [];
    if (!groups.length) {
      sequence += 1;
      const collectedAt = windowItem.last_collect_at || 0;
      const collectedAtText = collectedAt ? formatTimestamp(collectedAt) : "-";
      rows.push(`
        <tr>
          <td class="col-index">${sequence}</td>
          <td>${escapeHtml(windowToken || "-")}</td>
          <td>${escapeHtml(windowName)}</td>
          <td>${escapeHtml(resolveWindowCollectStatus(windowItem))}</td>
          <td>-</td>
          <td>${escapeHtml(collectedAtText)}</td>
          <td>-</td>
        </tr>
      `);
      return;
    }
    groups.forEach((group) => {
      sequence += 1;
      const groupId = String(group.group_id || "").trim();
      const groupName = resolveCollectedGroupName(group) || "-";
      const groupUrl = String(group.group_url || "").trim();
      const collectedAt = group.collected_at || windowItem.last_collect_at || 0;
      const collectedAtText = collectedAt ? formatTimestamp(collectedAt) : "-";
      const checked = Number(group.selected || 0) === 1 ? "checked" : "";
      const linkHtml = groupUrl
        ? `<a href="${escapeHtml(groupUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(groupUrl)}</a>`
        : "-";
      rows.push(`
        <tr>
          <td class="col-index">${sequence}</td>
          <td>${escapeHtml(windowToken || "-")}</td>
          <td>${escapeHtml(windowName)}</td>
          <td>${escapeHtml(groupName)}</td>
          <td>${linkHtml}</td>
          <td>${escapeHtml(collectedAtText)}</td>
          <td>
            <input
              type="checkbox"
              data-role="group-select"
              data-window-token="${escapeHtml(windowToken)}"
              data-group-id="${escapeHtml(groupId)}"
              ${checked}
            />
          </td>
        </tr>
      `);
    });
  });
  if (!rows.length) {
    setDeviceDetailEmpty(
      preferredWindowTokenText
        ? `当前设备在窗口 ${preferredWindowTokenText} 暂无采集结果`
        : "当前设备暂无窗口采集结果"
    );
    return;
  }
  deviceDetailTableBody.innerHTML = rows.join("");
  setButtonDisabled(deviceDetailSaveBtn, false, "");
}

async function loadDeviceDetail(machineId, owner = "") {
  const normalizedMachineId = String(machineId || "").trim();
  if (!normalizedMachineId) {
    await showAlert("缺少设备标识");
    return false;
  }
  const query = new URLSearchParams({ machine_id: normalizedMachineId });
  const normalizedOwner = String(owner || "").trim();
  if (normalizedOwner) {
    query.set("owner", normalizedOwner);
  }
  const response = await api(`/device-detail?${query.toString()}`);
  if (!response.ok) {
    await showAlert(translateErrorMessage(response.data?.error) || "设备详情读取失败");
    return false;
  }
  renderDeviceDetail(response.data || {});
  setActivePage("device-detail", "设备管理", false);
  return true;
}

function collectDeviceDetailSelections() {
  const visibleWindowTokens = new Set(
    (Array.isArray(currentDeviceDetailVisibleWindowTokens)
      ? currentDeviceDetailVisibleWindowTokens
      : []
    )
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  );
  const windows = (Array.isArray(currentDeviceDetail?.windows) ? currentDeviceDetail.windows : []).filter(
    (windowItem) => {
      if (!visibleWindowTokens.size) return true;
      const token = String(windowItem?.window_token || "").trim();
      return token && visibleWindowTokens.has(token);
    }
  );
  const selectionMap = new Map();
  windows.forEach((windowItem) => {
    const windowToken = String(windowItem.window_token || "").trim();
    if (!windowToken) return;
    selectionMap.set(windowToken, []);
  });
  if (deviceDetailTableBody) {
    const checkedNodes = Array.from(
      deviceDetailTableBody.querySelectorAll('input[data-role="group-select"]:checked')
    );
    checkedNodes.forEach((node) => {
      const windowToken = String(node.dataset.windowToken || "").trim();
      const groupId = String(node.dataset.groupId || "").trim();
      if (!windowToken || !groupId) return;
      if (!selectionMap.has(windowToken)) {
        selectionMap.set(windowToken, []);
      }
      selectionMap.get(windowToken).push(groupId);
    });
  }
  return Array.from(selectionMap.entries()).map(([windowToken, groupIds]) => ({
    window_token: windowToken,
    group_ids: groupIds,
  }));
}

async function saveDeviceDetailSelections() {
  const detail = currentDeviceDetail || {};
  const device = detail.device || {};
  const owner = String(device.owner || detail.owner || "").trim();
  const machineId = String(device.machine_id || detail.machine_id || "").trim();
  if (!machineId) {
    await showAlert("请先选择设备");
    return;
  }
  const selections = collectDeviceDetailSelections();
  const response = await api("/device-groups/save", {
    method: "POST",
    body: {
      owner,
      machine_id: machineId,
      selections,
    },
  });
  if (!response.ok) {
    await showAlert(translateErrorMessage(response.data?.error) || "保存失败");
    return;
  }
  await showAlert("群组选择已保存");
  await loadDeviceDetail(machineId, owner);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function renderCopyCell(value, options = {}) {
  const text = value === null || value === undefined || value === "" ? "-" : String(value);
  const monoClass = options.mono === false ? "" : " mono";
  const safeText = escapeHtml(text);
  return `
    <div class="copyable-cell">
      <span class="copyable-text${monoClass}">${safeText}</span>
    </div>
  `;
}

window.addEventListener("resize", () => {
  if (lastTrend) renderTrendChart(lastTrend);
});

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") {
    void refreshStatus({ forceAux: true, includeWhenHidden: true });
  }
});

async function api(url, options = {}) {
  const init = {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
    },
    credentials: "include",
  };

  if (options.body) {
    init.body = JSON.stringify(options.body);
  }

  let response = null;
  let data = null;
  try {
    response = await fetch(resolveApiUrl(url), init);
    try {
      data = await response.json();
    } catch (error) {
      data = null;
    }
  } catch (error) {
    return { ok: false, status: 0, data: { error: "网络请求失败，请稍后重试" } };
  }

  const logicalOk =
    response.ok &&
    (!data || (data.ok !== false && typeof data.error === "undefined"));
  return { ok: logicalOk, status: response.status, data };
}

function resolveApiUrl(url) {
  if (!url.startsWith("/")) return url;
  if (url.startsWith("/api/admin") || url.startsWith("/api/sub") || url.startsWith("/api/agent")) {
    return url;
  }
  return `${API_PREFIX}${url}`;
}

initializeApp();
