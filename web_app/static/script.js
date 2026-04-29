// ═══════════════════════════════════════════════════════
//  Chi Phí Ads Dashboard — script.js
//  Supports 3 roles: admin | lead | employee
// ═══════════════════════════════════════════════════════

const ROLE      = window.APP_ROLE      || "employee";
const TEAM      = window.APP_TEAM      || "";
const DISPLAY   = window.APP_DISPLAY   || "";
const SHEET_URL = window.APP_SHEET_URL || "";
const PERFORMANCE_SHEET_URL = window.APP_PERFORMANCE_SHEET_URL || "";
const SHEETS    = window.APP_SHEETS    || [];
const MONTHLY_SHEETS = window.APP_MONTHLY_SHEETS || [];
const MONTHLY_PERFORMANCE_SHEETS = window.APP_MONTHLY_PERFORMANCE_SHEETS || [];
const SESSION_TIMEOUT_MS = Math.max(60, Number(window.APP_SESSION_TIMEOUT_SECONDS || 600)) * 1000;
const SESSION_KEEPALIVE_MS = 60 * 1000;

// ─── State ────────────────────────────────────────────
let currentData  = { rows: [], headers: [], ads_percent: "", memberSummaries: [] };
let filteredRows = [];
let currentPerformanceMetrics = null;
let autoFillEnabled = true;
let charts = { spendByDate: null, spendByProduct: null };
let currentPage = 1;
let pageSize = 50;
let inactivityTimer = null;
let lastKeepAliveAt = 0;
let activeSheetInputId = "sheetUrl"; // Track which URL input is active

// ─── Init ─────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    setupInactivityLogout();
    initMetaApiHelperModal();
    populateMemberSelect();
    populateMonthSelect();
    await loadAutoFillStatus();
    initURLInputListeners();

    if (ROLE === "employee") {
        const defaultSheet = SHEET_URL || (SHEETS[0] && SHEETS[0].url) || "";
        const sheetInput = document.getElementById("sheetUrl");
        if (sheetInput && defaultSheet) sheetInput.value = defaultSheet;
        const performanceInput = document.getElementById("performanceSheetUrl");
        if (performanceInput && PERFORMANCE_SHEET_URL) performanceInput.value = PERFORMANCE_SHEET_URL;
        const sel = document.getElementById("memberSelect");
        if (sel && defaultSheet) sel.value = defaultSheet;

        if (defaultSheet) {
            await fetchAndRender(defaultSheet, false);
        } else {
            showError("⚠️ Vui lòng nhập Link chi phí ads để bắt đầu.");
        }
        return;
    }

    if ((ROLE === "admin" || ROLE === "lead") && document.getElementById("btnLoadAll")) {
        await loadAllData();
    }
});

// ─── Member dropdown ───────────────────────────────────
function populateMemberSelect() {
    const sel = document.getElementById("memberSelect");
    if (!sel || !SHEETS.length) return;
    SHEETS.forEach(s => {
        const opt = document.createElement("option");
        opt.value = s.url;
        opt.textContent = ROLE === "admin" ? `[${s.team || "?"}] ${s.name}` : s.name;
        sel.appendChild(opt);
    });
}

function loadMemberSheet(url) {
    if (!url) return;
    fetchAndRender(url, false);
}

function populateMonthSelect() {
    const sel = document.getElementById("monthSelect");
    if (!sel || ROLE !== "employee") return;

    sel.innerHTML = '<option value="">-- Chọn tháng --</option>';
    MONTHLY_SHEETS.forEach(item => {
        const opt = document.createElement("option");
        opt.value = item.month_key;
        opt.textContent = item.month_label || item.month_key;
        sel.appendChild(opt);
    });

    if (MONTHLY_SHEETS.length > 0) {
        sel.value = MONTHLY_SHEETS[0].month_key;
        const input = document.getElementById("sheetUrl");
        if (input && !input.value && MONTHLY_SHEETS[0].sheet_url) {
            input.value = MONTHLY_SHEETS[0].sheet_url;
        }
    }
}

function loadMonthSheet(monthKey) {
    if (!monthKey) return;
    const found = MONTHLY_SHEETS.find(m => m.month_key === monthKey);
    if (!found || !found.sheet_url) {
        showError("⚠️ Chưa có sheet cho tháng đã chọn.");
        return;
    }
    const input = document.getElementById("sheetUrl");
    if (input) input.value = found.sheet_url;
    fetchAndRender(found.sheet_url, false);
}

function openMonthFolder() {
    const sel = document.getElementById("monthSelect");
    const key = sel ? sel.value : "";
    if (!key) {
        showToast("⚠️ Vui lòng chọn tháng trước.");
        return;
    }
    window.open(`/monthly-folder/${encodeURIComponent(key)}`, "_blank");
}

function initMetaApiHelperModal() {
    const modal = document.getElementById("metaApiHelperModal");
    const openBtn = document.getElementById("openMetaApiHelperBtn");
    const closeBtn = document.getElementById("closeMetaApiHelperBtn");
    const runBtn = document.getElementById("runMetaApiAutoBtn");
    if (!modal || !openBtn || !closeBtn || !runBtn) return;

    const openModal = () => {
        modal.classList.add("show");
        modal.setAttribute("aria-hidden", "false");
    };
    const closeModal = () => {
        modal.classList.remove("show");
        modal.setAttribute("aria-hidden", "true");
    };

    openBtn.addEventListener("click", openModal);
    closeBtn.addEventListener("click", closeModal);
    modal.addEventListener("click", (e) => {
        if (e.target === modal) closeModal();
    });
    document.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && modal.classList.contains("show")) {
            closeModal();
        }
    });

    runBtn.addEventListener("click", async () => {
        const sheetUrl = (document.getElementById("sheetUrl")?.value || "").trim();
        if (!sheetUrl) {
            showToast("⚠️ Vui lòng nhập link Sheet trước khi đồng bộ API.");
            return;
        }
        runBtn.disabled = true;
        const original = runBtn.innerHTML;
        runBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang đồng bộ...';
        try {
            await fetchAndRender(sheetUrl, false);
            showToast("✅ Đã chạy đồng bộ API tự động.");
            closeModal();
        } finally {
            runBtn.disabled = false;
            runBtn.innerHTML = original;
        }
    });
}

// ─── Load ALL sheets (lead/admin) ─────────────────────
async function loadAllData() {
    const btn = document.getElementById("btnLoadAll");
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang tải...'; }
    const spinner = document.getElementById("loadingSpinner");
    if (spinner) spinner.style.display = "block";
    document.getElementById("errorMessage").style.display = "none";

    try {
        const res  = await fetch("/api/fetch-all-data", { method: "POST" });
        if (await handleSessionExpiredGateFromResponse(res)) return;
        const data = await res.json();
        if (spinner) spinner.style.display = "none";
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-layer-group"></i> Tải báo cáo tổng'; }

        if (handleTelegramSetupGate(data, res.status)) return;

        if (!data.success) { showError("❌ " + (data.error || "Không thể tải dữ liệu tổng")); return; }
        if (!data.data || data.data.length === 0) { showError("⚠️ Chưa có dữ liệu trong các sheet."); return; }

        currentData = {
            rows: data.data,
            headers: data.headers,
            ads_percent: "",
            profitability_metrics: null,
            account_summary: [],
            memberSummaries: data.member_summaries || [],
        };
        currentPerformanceMetrics = null;
        filteredRows = [...currentData.rows];
        resetDateInputs();
        renderData();
        hideAccountStatusPanel();

        if (data.errors && data.errors.length) {
            showToast("⚠️ Không tải được: " + data.errors.map(e => e.name).join(", "));
        }
    } catch (e) {
        if (spinner) spinner.style.display = "none";
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-layer-group"></i> Tải báo cáo tổng'; }
        showError("❌ Lỗi kết nối: " + e.message);
    }
}

// ─── Fetch single sheet ───────────────────────────────
async function fetchAndRender(sheetUrl, shouldAutoSave = true) {
    const spinner  = document.getElementById("loadingSpinner");
    const errorDiv = document.getElementById("errorMessage");
    errorDiv.style.display = "none";
    if (spinner) spinner.style.display = "block";

    try {

function copyTextSafe(text) {
    const value = String(text || "").trim();
    if (!value) return Promise.resolve(false);
    if (!navigator.clipboard || !navigator.clipboard.writeText) return Promise.resolve(false);
    return navigator.clipboard.writeText(value).then(() => true).catch(() => false);
}

function maybeAutoOpenSheetForAccess(data) {
    if (!data || !data.can_auto_open_sheet) return;
    const sheetId = String(data.sheet_id || "").trim();
    if (!sheetId) return;

    const storageKey = `sheet_access_opened_${sheetId}`;
    if (sessionStorage.getItem(storageKey) === "1") return;

    const targetUrl = (data.share_url || data.request_access_url || data.clean_url || "").trim();
    if (!targetUrl) return;

    sessionStorage.setItem(storageKey, "1");
    try {
        window.open(targetUrl, "_blank", "noopener,noreferrer");
        showToast("Đã mở Google Sheet để bạn cấp quyền hoặc gửi yêu cầu truy cập.");
    } catch (_) {}

    const serviceEmail = String(data.service_account_email || "").trim();
    if (serviceEmail) {
        copyTextSafe(serviceEmail).then((copied) => {
            if (copied) {
                showToast("Đã copy email service account để bạn dán vào ô Chia sẻ.");
            }
        });
    }
}
        const res  = await fetch("/api/fetch-data", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sheet_url: sheetUrl, sync_meta: true }),
        });
    if (await handleSessionExpiredGateFromResponse(res)) return;
        const data = await res.json();
        if (spinner) spinner.style.display = "none";

    const extraLines = [];
    if (data.service_account_email) {
        extraLines.push(`Email service account: ${data.service_account_email}`);
    }
    if (data.share_url) {
        extraLines.push(`Mở nhanh để Chia sẻ: ${data.share_url}`);
    }
    if (data.request_access_url) {
        extraLines.push(`Link yêu cầu quyền truy cập: ${data.request_access_url}`);
    }

        if (handleTelegramSetupGate(data, res.status)) return;

        if (!data.success) {
            const detail = buildSheetAccessHint(data);
            maybeAutoOpenSheetForAccess(data);
            showError("❌ " + (data.error || "Không thể tải dữ liệu") + (detail ? `\n${detail}` : ""));
            return;
        }
        if (!data.data || data.data.length === 0) {
            hideAccountStatusPanel();
            showError("⚠️ Sheet không có dữ liệu trong tab 'Chi phí ADS'");
            return;
        }

        currentData = {
            rows: data.data,
            headers: data.headers,
            ads_percent: data.ads_percent || "",
            profitability_metrics: data.profitability_metrics || null,
            account_summary: data.account_summary || [],
            memberSummaries: [],
        };
        filteredRows = [...currentData.rows];
        resetDateInputs();
        renderData();

        const syncMeta = data.sync_meta || {};
        if (syncMeta.attempted) {
            if (Number(syncMeta.written_rows || 0) > 0) {
                showToast(`✅ Đã tự đồng bộ Meta API: ${syncMeta.written_rows} dòng chi phí.`);
            } else if (syncMeta.hint) {
                showToast(`⚠️ ${syncMeta.hint}`);
            }
        }

        const perfInputUrl = (document.getElementById("performanceSheetUrl")?.value || "").trim();
        const perfUrl = perfInputUrl || sheetUrl;
        await loadPerformanceSummary(perfUrl);
        await loadAccountStatuses(sheetUrl);

        if (shouldAutoSave) saveSheetUrl(sheetUrl);
    } catch (e) {
        if (spinner) spinner.style.display = "none";
        showError("❌ Lỗi kết nối: " + e.message);
    }
}

async function handleSubmit(event) {
    event.preventDefault();
    const sheetUrl = (document.getElementById("sheetUrl")?.value || "").trim();
    const performanceSheetUrl = (document.getElementById("performanceSheetUrl")?.value || "").trim();
    await fetchAndRender(sheetUrl, false);
    await saveSheetUrl(sheetUrl, performanceSheetUrl);
}

async function loadPerformanceSummary(performanceSheetUrl) {
    if (!performanceSheetUrl) {
        currentPerformanceMetrics = null;
        renderStats(filteredRows);
        return;
    }

    try {
        const res = await fetch("/api/performance-summary", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ performance_sheet_url: performanceSheetUrl }),
        });
        if (await handleSessionExpiredGateFromResponse(res)) return;
        const data = await res.json();
        if (!data.success) {
            currentPerformanceMetrics = null;
            renderStats(filteredRows);
            const detail = buildSheetAccessHint(data);
            maybeAutoOpenSheetForAccess(data);
            if (data.error) {
                showError(`⚠️ ${data.error}${detail ? `\n${detail}` : ""}`);
            }
            return;
        }

        currentPerformanceMetrics = data.metrics || null;
        renderStats(filteredRows);
    } catch (_) {
        currentPerformanceMetrics = null;
        renderStats(filteredRows);
    }
}

// ─── Date Filter ──────────────────────────────────────
function parseViDate(s) {
    if (!s) return null;
    if (s.includes("/")) {
        const [d, m, y] = s.split("/");
        if (d && m && y) return new Date(`${y.padStart(4,"0")}-${m.padStart(2,"0")}-${d.padStart(2,"0")}`);
    }
    const dt = new Date(s);
    return isNaN(dt) ? null : dt;
}

function applyDateFilter() {
    const fromVal = document.getElementById("dateFrom").value;
    const toVal   = document.getElementById("dateTo").value;
    if (!fromVal && !toVal) { resetDateFilter(); return; }

    const fromDate = fromVal ? new Date(fromVal) : null;
    const toDate   = toVal   ? new Date(toVal)   : null;
    if (toDate) toDate.setHours(23, 59, 59);

    filteredRows = currentData.rows.filter(row => {
        const d = parseViDate(row["Ngày"] || "");
        if (!d) return true;
        if (fromDate && d < fromDate) return false;
        if (toDate   && d > toDate)   return false;
        return true;
    });

    const info = document.getElementById("filterInfo");
    if (info) {
        info.textContent = `Đang lọc: ${fromVal || "—"} → ${toVal || "—"} | ${filteredRows.length} dòng`;
        info.style.display = "block";
    }
    currentPage = 1;
    renderStats(filteredRows);
    renderTable(filteredRows);
    renderInsights(filteredRows);
}

function resetDateFilter() {
    filteredRows = [...currentData.rows];
    currentPage = 1;
    resetDateInputs();
    const info = document.getElementById("filterInfo");
    if (info) info.style.display = "none";
    renderStats(filteredRows);
    renderTable(filteredRows);
    renderInsights(filteredRows);
}

function resetDateInputs() {
    const f = document.getElementById("dateFrom");
    const t = document.getElementById("dateTo");
    if (f) f.value = "";
    if (t) t.value = "";
    const info = document.getElementById("filterInfo");
    if (info) info.style.display = "none";
}

// ─── Render ───────────────────────────────────────────
function renderData() {
    if (!currentData.rows || currentData.rows.length === 0) { showError("Không có dữ liệu trong sheet"); return; }
    filteredRows = [...currentData.rows];
    currentPage = 1;
    document.getElementById("statsSection").style.display = "block";
    document.getElementById("tableSection").style.display = "block";
    document.getElementById("chartsSection").style.display = "grid";
    const rankSec = document.getElementById("rankingsSection");
    if (rankSec) rankSec.style.display = currentData.memberSummaries.length ? "block" : "none";

    // Keep core data visible even if a secondary renderer (e.g. chart CDN) fails.
    try { renderStats(filteredRows); } catch (_) {}
    try { renderRankings(); } catch (_) {}
    try { renderTable(filteredRows); } catch (_) {}
    try { renderInsights(filteredRows); } catch (_) {
        const chartSection = document.getElementById("chartsSection");
            // Removed the call to loadAccountStatuses after reading dashboard data
        showToast("⚠️ Không tải được khung tổng hợp. Dữ liệu bảng vẫn hiển thị bình thường.");
    }
}

function renderStats(rows) {
    const profitability = currentData.profitability_metrics || null;
    const completionMetrics = profitability?.completion_percent || null;
    const grossProfitMetrics = profitability?.gross_profit || null;
    const grossProfitPercentMetrics = profitability?.gross_profit_percent || null;

    if (currentPerformanceMetrics) {
        const revenue = currentPerformanceMetrics.revenue || null;
        const spend = currentPerformanceMetrics.total_spend || { month: 0, day: 0, unit: "VND" };
        const results = currentPerformanceMetrics.total_results || { month: 0, day: 0, unit: "data" };
        const cpr = currentPerformanceMetrics.cost_per_result || { month: 0, day: 0, unit: "VND" };
        const ads = currentPerformanceMetrics.ads_percent || { month: 0, day: 0, unit: "%" };
        const avgOrder = currentPerformanceMetrics.avg_order_value || { month: 0, day: 0, unit: "VND" };

        if (revenue) {
            setStatValue("totalSpend", formatMetricNumber(revenue.month), revenue.unit || "VND", "Hôm nay", formatMetricNumber(revenue.day));
        } else {
            setStatValue("totalSpend", "—", "", "", "");
        }
        setStatValue("totalResults", formatMetricNumber(results.month, false), results.unit, "Hôm nay", formatMetricNumber(results.day, false));
        setStatValue("costPerResult", formatMetricNumber(cpr.month), cpr.unit, "Hôm nay", formatMetricNumber(cpr.day));
        setStatValue("adsPercent", formatMetricNumber(ads.month, true), ads.unit, "Hôm nay", formatMetricNumber(ads.day, true));
        setStatValue("avgOrderValue", formatMetricNumber(avgOrder.month), avgOrder.unit, "Hôm nay", formatMetricNumber(avgOrder.day));

        if (completionMetrics) {
            setStatValue(
                "completionPercent",
                formatMetricNumber(completionMetrics.total, true),
                completionMetrics.unit || "%",
                "",
                ""
            );
        } else {
            setStatValue("completionPercent", "—", "%", "", "");
        }

        if (grossProfitMetrics) {
            setStatValue(
                "grossProfit",
                formatMetricNumber(grossProfitMetrics.total),
                grossProfitMetrics.unit || "VND",
                "%LN gộp",
                grossProfitPercentMetrics ? formatMetricNumber(grossProfitPercentMetrics.total, true) : "—"
            );
        } else {
            setStatValue("grossProfit", "—", "", "%LN gộp", "—");
        }
        return;
    }

    const sourceRows = currentData.rows || rows || [];
    const today = new Date();
    const todayKey = `${String(today.getDate()).padStart(2, "0")}/${String(today.getMonth() + 1).padStart(2, "0")}/${today.getFullYear()}`;

    let totalSpend = 0;
    let totalResults = 0;
    let todaySpend = 0;
    let todayResults = 0;

    sourceRows.forEach(row => {
        const spendVal = parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
        const resultVal = parseIntJS(row["Số Data"] || "");
        const rowDate = (row["Ngày"] || "").trim();

        totalSpend += spendVal;
        totalResults += resultVal;

        if (rowDate === todayKey) {
            todaySpend += spendVal;
            todayResults += resultVal;
        }
    });

    const costPerResult = totalResults > 0 ? Math.round(totalSpend / totalResults) : 0;
    const todayCostPerResult = todayResults > 0 ? Math.round(todaySpend / todayResults) : 0;

    setStatValue("totalSpend", "—", "", "", "");
    setStatValue("totalResults", totalResults.toLocaleString("en-US"), "data", "Hôm nay", todayResults.toLocaleString("en-US"));
    setStatValue("costPerResult", costPerResult.toLocaleString("vi-VN"), "VND", "Hôm nay", todayCostPerResult.toLocaleString("vi-VN"));

    const adsRaw = (currentData.ads_percent || "").toString().trim();
    const adsMain = (!adsRaw || adsRaw === "—") ? "—" : (adsRaw.endsWith("%") ? adsRaw.slice(0, -1) : adsRaw);
    setStatValue("adsPercent", adsMain, "%", "Hôm nay", "—");
    setStatValue("avgOrderValue", "—", "", "Hôm nay", "—");

    if (completionMetrics) {
        setStatValue(
            "completionPercent",
            formatMetricNumber(completionMetrics.total, true),
            completionMetrics.unit || "%",
            "",
            ""
        );
    } else {
        setStatValue("completionPercent", "—", "%", "", "");
    }

    if (grossProfitMetrics) {
        setStatValue(
            "grossProfit",
            formatMetricNumber(grossProfitMetrics.total),
            grossProfitMetrics.unit || "VND",
            "%LN gộp",
            grossProfitPercentMetrics ? formatMetricNumber(grossProfitPercentMetrics.total, true) : "—"
        );
    } else {
        setStatValue("grossProfit", "—", "", "%LN gộp", "—");
    }
}

function setStatValue(elementId, mainValue, unit = "", subLabel = "", subValue = "") {
    const el = document.getElementById(elementId);
    if (!el) return;
    const safeMain = escapeHtml(mainValue || "—");
    const safeUnit = escapeHtml(unit || "");
    const safeSubLabel = escapeHtml(subLabel || "");
    const safeSubValue = escapeHtml(subValue || "");

    const mainBlock = (safeSubValue && safeSubValue !== "—")
        ? `<span class="value-stack"><span class="value-main">${safeMain}</span><span class="value-sub">${safeSubLabel}: ${safeSubValue}</span></span>`
        : `<span class="value-main">${safeMain}</span>`;

    if (!safeUnit) {
        el.innerHTML = mainBlock;
        return;
    }

    el.innerHTML = `${mainBlock}<span class="value-unit">${safeUnit}</span>`;
}

function formatMetricNumber(value, isPercent = false) {
    const n = Number(value || 0);
    if (isPercent) {
        return n.toLocaleString("vi-VN", { minimumFractionDigits: 0, maximumFractionDigits: 2 });
    }
    if (Math.abs(n) >= 1_000_000_000) {
        return (n / 1_000_000_000).toLocaleString("vi-VN", { minimumFractionDigits: 1, maximumFractionDigits: 2 }) + " tỷ";
    }
    if (Math.abs(n) >= 1_000_000) {
        return (n / 1_000_000).toLocaleString("vi-VN", { minimumFractionDigits: 0, maximumFractionDigits: 1 }) + " tr";
    }
    return Math.round(n).toLocaleString("vi-VN");
}

function renderRankings() {
    const tbody = document.getElementById("rankingsBody");
    if (!tbody) return;
    const summaries = currentData.memberSummaries;
    if (!summaries || !summaries.length) return;
    const isAdmin = ROLE === "admin";
    tbody.innerHTML = summaries.map((m, idx) => {
        const icon    = idx === 0 ? "🥇" : idx === 1 ? "🥈" : idx === 2 ? "🥉" : `#${m.rank}`;
        const teamCol = isAdmin ? `<td>${m.team || "—"}</td>` : "";
        return `<tr class="${idx < 3 ? "rank-top" : ""}">
            <td class="rank-cell">${icon}</td>
            ${teamCol}
            <td>${m.name}</td>
            <td class="spend-cell">${formatCurrency(m.total_spend)}</td>
            <td>${m.total_data.toLocaleString("en-US")}</td>
            <td>${formatCurrency(m.cost_per_data)}</td>
        </tr>`;
    }).join("");
}

async function loadAccountStatuses(sheetUrl) {
    if (!sheetUrl) {
        hideAccountStatusPanel();
        return;
    }

    try {
        const res = await fetch("/api/account-status", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sheet_url: sheetUrl }),
        });
        if (await handleSessionExpiredGateFromResponse(res)) return;

        const data = await res.json();
        if (!data.success) {
            hideAccountStatusPanel();
            if (data.error) showToast(`⚠️ ${data.error}`);
            return;
        }

        renderAccountStatuses(data);
    } catch (_) {
        hideAccountStatusPanel();
    }
}

function hideAccountStatusPanel() {
    const section = document.getElementById("accountStatusSection");
    if (section) section.style.display = "none";
}

function renderAccountStatuses(payload) {
    const section = document.getElementById("accountStatusSection");
    const tbody = document.getElementById("accountStatusBody");
    if (!section || !tbody) return;

    const summary = payload.summary || {};
    const accounts = Array.isArray(payload.accounts) ? payload.accounts : [];

    const totalEl = document.getElementById("accountStatusTotal");
    const hasSpendEl = document.getElementById("accountStatusHasSpend");
    const noSpendEl = document.getElementById("accountStatusNoSpend");
    const notConnectedEl = document.getElementById("accountStatusNotConnected");

    if (totalEl) totalEl.textContent = String(summary.total || accounts.length || 0);
    if (hasSpendEl) hasSpendEl.textContent = String(summary.has_spend || 0);
    if (noSpendEl) noSpendEl.textContent = String(summary.no_spend || 0);
    if (notConnectedEl) notConnectedEl.textContent = String(summary.not_connected || 0);

    if (!accounts.length) {
        tbody.innerHTML = '<tr><td class="account-status-empty" colspan="6">Không tìm thấy tài khoản quảng cáo trong tab Cài đặt.</td></tr>';
        section.style.display = "block";
        return;
    }

    tbody.innerHTML = accounts.map((acc) => {
        const status = String(acc.status || "not_connected");
        const statusLabel = escapeHtml(acc.status_label || "Chưa rõ");
        const hint = escapeHtml(acc.hint || "");
        const spend = Number(acc.spend_today || 0);
        const spendLabel = spend > 0 ? formatCurrency(spend) : "0 VND";
        const isConnected = status === "has_spend" || status === "no_spend";
        const apiBadge = isConnected
            ? `<span class="api-connect-badge connected"><i class="fas fa-check-circle"></i> Đã kết nối</span>`
            : `<span class="api-connect-badge disconnected"><i class="fas fa-times-circle"></i> Chưa kết nối</span>`;
        return `<tr>
            <td>${escapeHtml(acc.account_name || "—")}</td>
            <td>act_${escapeHtml(acc.account_id || "")}</td>
            <td>${apiBadge}</td>
            <td class="spend-cell">${spendLabel}</td>
            <td><span class="account-status-badge ${status.replace(/_/g, "-")}">${statusLabel}</span></td>
            <td class="account-hint">${hint || "—"}</td>
        </tr>`;
    }).join("");

    section.style.display = "block";
}

function renderTable(rows) {
    const headers = currentData.headers;
    const totalRows = rows.length;
    const pageRows = rows;

    const COMPUTED_COL = "Chi phí/KQ (USD)";
    const headerHTML = headers.map(h => `<th>${h}</th>`).join("") + `<th>${COMPUTED_COL}</th>`;
    document.getElementById("tableHeader").innerHTML = headerHTML;

    document.getElementById("tableBody").innerHTML = pageRows.map(row => {
        const cells = headers.map(h => `<td>${row[h] || "-"}</td>`).join("");
        const usdRaw = row["Số tiền chi tiêu - USD"] || "";
        const usdVal = parseSpendJS(usdRaw);
        const dataVal = parseInt(row["Số Data"] || "0", 10) || 0;
        const cpr = (usdVal > 0 && dataVal > 0) ? (usdVal / dataVal).toFixed(3) : "-";
        return `<tr>${cells}<td>${cpr}</td></tr>`;
    }).join("");

    updatePagination(totalRows, pageRows.length);
}

function updatePagination(totalRows, currentCount) {
    const pageInfo = document.getElementById("pageInfo");
    const prevBtn = document.getElementById("prevPageBtn");
    const nextBtn = document.getElementById("nextPageBtn");
    const countInfo = document.getElementById("tableCountInfo");
    const paginationBar = document.querySelector(".pagination-bar");
    const pageSizeWrap = document.querySelector(".table-page-size-wrap");

    if (paginationBar) paginationBar.style.display = "none";
    if (pageSizeWrap) pageSizeWrap.style.display = "none";

    if (pageInfo) pageInfo.textContent = "Hiển thị toàn bộ";
    if (prevBtn) prevBtn.disabled = true;
    if (nextBtn) nextBtn.disabled = true;
    if (countInfo) {
        const from = totalRows === 0 ? 0 : 1;
        const to = totalRows === 0 ? 0 : currentCount;
        countInfo.textContent = `${from}-${to} / ${totalRows} dòng`;
    }
}

function prevPage() {
    if (currentPage <= 1) return;
    currentPage -= 1;
    renderTable(filteredRows);
}

function nextPage() {
    const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
    if (currentPage >= totalPages) return;
    currentPage += 1;
    renderTable(filteredRows);
}

function changePageSize(value) {
    const parsed = parseInt(value, 10);
    pageSize = Number.isFinite(parsed) && parsed > 0 ? parsed : 50;
    currentPage = 1;
    renderTable(filteredRows);
}

function renderInsights(rows) {
    const accountBox = document.getElementById("accountInsightList");
    const weeklyBox = document.getElementById("weeklyInsightList");
    const lngBox = document.getElementById("lngInsightList");
    if (!accountBox || !weeklyBox || !lngBox) return;

    renderAccountInsight(accountBox, rows);
    renderWeeklyInsight(weeklyBox);
    renderLNGInsight(lngBox);
}

function renderAccountInsight(container, rows) {
    const source = Array.isArray(currentData.account_summary) && currentData.account_summary.length
        ? currentData.account_summary
        : buildAccountInsightFallback(rows || []);

    if (!source.length) {
        container.innerHTML = '<div class="insight-empty">Chưa có dữ liệu tài khoản quảng cáo.</div>';
        return;
    }

    container.innerHTML = source.map((item) => {
        const liveClass = item.is_live ? "live" : "die";
        return `<div class="insight-row">
            <div class="insight-main">
                <span class="insight-dot ${liveClass}"></span>
                <span class="insight-name">${escapeHtml(item.account_name || "Không rõ tài khoản")}</span>
            </div>
            <span class="insight-meta">${formatCurrency(item.total_spend || 0)}</span>
        </div>`;
    }).join("");
}

function buildAccountInsightFallback(rows) {
    const now = new Date();
    const todayKey = `${String(now.getDate()).padStart(2, "0")}/${String(now.getMonth() + 1).padStart(2, "0")}/${now.getFullYear()}`;
    const byAcc = {};

    rows.forEach((row) => {
        const name = (row["Tên tài khoản"] || "").trim() || "Không rõ tài khoản";
        const spend = parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
        const day = (row["Ngày"] || "").trim();

        if (!byAcc[name]) byAcc[name] = { account_name: name, total_spend: 0, today_spend: 0, is_live: false };
        byAcc[name].total_spend += spend;
        if (day === todayKey) byAcc[name].today_spend += spend;
    });

    const list = Object.values(byAcc).map((item) => ({
        account_name: item.account_name,
        total_spend: Math.round(item.total_spend),
        today_spend: Math.round(item.today_spend),
        is_live: item.today_spend > 0,
    }));

    list.sort((a, b) => Number(b.total_spend || 0) - Number(a.total_spend || 0));
    return list;
}

function renderWeeklyInsight(container) {
    const weekly = Array.isArray(currentPerformanceMetrics?.weekly_trend) ? currentPerformanceMetrics.weekly_trend : [];
    if (!weekly.length) {
        container.innerHTML = '<div class="insight-empty">Chưa có dữ liệu 7 ngày từ bảng hiệu suất.</div>';
        return;
    }

    const now = new Date();
    const todayKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;

    container.innerHTML = weekly.map((item) => {
        const isToday = item.date_key === todayKey;
        const noData = !item.data && !item.revenue;
        const rowStyle = isToday ? ' style="background:#eef9f0;border-color:#86efac;font-weight:700;"' : (noData ? ' style="opacity:0.5;"' : "");
        const label = isToday ? `${escapeHtml(item.date || "—")} ◀ Hôm nay` : escapeHtml(item.date || "—");
        const meta = noData
            ? "Không có dữ liệu"
            : `Data: ${formatMetricNumber(item.data, false)} | DS: ${formatMetricNumber(item.revenue)} | %Ads: ${formatMetricNumber(item.ads_percent, true)}%`;
        return `<div class="insight-row"${rowStyle}>
            <div class="insight-main"><span class="insight-name">${label}</span></div>
            <span class="insight-meta">${meta}</span>
        </div>`;
    }).join("");
}

function renderLNGInsight(container) {
    const lng = currentData.profitability_metrics?.product_lng || {};
    // Support both new format {items:[]} and legacy {top:[], bottom:[]}
    let items = null;
    if (Array.isArray(lng.items)) {
        items = lng.items;
    } else {
        const top = Array.isArray(lng.top) ? lng.top : [];
        const bottom = Array.isArray(lng.bottom) ? lng.bottom : [];
        if (!top.length && !bottom.length) {
            container.innerHTML = '<div class="insight-empty">Chưa có dữ liệu LNG sản phẩm trong tab LN gộp dự tính.</div>';
            return;
        }
        // Merge legacy and sort desc
        const merged = [...top, ...bottom.filter(b => !top.some(t => t.product_name === b.product_name))];
        items = merged.sort((a, b) => (b.lng || 0) - (a.lng || 0));
    }

    if (!items || !items.length) {
        container.innerHTML = '<div class="insight-empty">Chưa có dữ liệu LNG sản phẩm trong tab LN gộp dự tính.</div>';
        return;
    }

    container.innerHTML = items.map(item => {
        const lngVal = item.lng || 0;
        const pctText = (item.lng_pct != null) ? ` <span style="font-size:0.82em;color:#64748b;">(${item.lng_pct}%)</span>` : "";
        const color = lngVal >= 0 ? "#16a34a" : "#dc2626";
        const displayName = item.product_name_vn || item.product_name || "—";
        return `<div class="insight-row">
            <div class="insight-main"><span class="insight-name">${escapeHtml(displayName)}</span></div>
            <span class="insight-meta" style="color:${color};white-space:nowrap;">${formatCurrency(lngVal)}${pctText}</span>
        </div>`;
    }).join("");
}

// ─── URL suggestions ──────────────────────────────────
function initURLInputListeners() {
    const sheetInput = document.getElementById("sheetUrl");
    const perfInput = document.getElementById("performanceSheetUrl");
    const box = document.getElementById("sheetSuggestions");
    const wrap = document.querySelector(".input-wrap");
    const autoBtn = document.getElementById("autoFillToggleBtn");
    
    if (autoBtn) autoBtn.addEventListener("click", toggleAutoFillStatus);
    if (!box || !wrap) return;

    const syncSuggestionBoxToInput = (inputEl) => {
        if (!inputEl) return;
        const wrapRect = wrap.getBoundingClientRect();
        const inputRect = inputEl.getBoundingClientRect();
        box.style.left = `${Math.max(0, inputRect.left - wrapRect.left)}px`;
        box.style.width = `${Math.max(180, inputRect.width)}px`;
    };
    
    const show = (inputId) => () => {
        activeSheetInputId = inputId;
        const targetInput = document.getElementById(inputId);
        syncSuggestionBoxToInput(targetInput);
        renderSuggestions(targetInput?.value || "");
        box.style.display = "block";
    };
    const hide = () => { setTimeout(() => { box.style.display = "none"; }, 150); };
    
    // Ads sheet input listeners
    if (sheetInput) {
        sheetInput.addEventListener("focus", show("sheetUrl"));
        sheetInput.addEventListener("click", show("sheetUrl"));
        sheetInput.addEventListener("input", show("sheetUrl"));
        sheetInput.addEventListener("blur", hide);
    }
    
    // Performance sheet input listeners
    if (perfInput) {
        perfInput.addEventListener("focus", show("performanceSheetUrl"));
        perfInput.addEventListener("click", show("performanceSheetUrl"));
        perfInput.addEventListener("input", show("performanceSheetUrl"));
        perfInput.addEventListener("blur", hide);
    }
    
    document.addEventListener("click", e => { if (!wrap.contains(e.target)) box.style.display = "none"; });
    window.addEventListener("resize", () => {
        if (box.style.display !== "none") {
            syncSuggestionBoxToInput(document.getElementById(activeSheetInputId));
        }
    });
}

function getSuggestionSource() {
    // If performance sheet input is active, return performance sheet suggestions
    if (activeSheetInputId === "performanceSheetUrl") {
        const seen = new Set();
        const source = [];
        MONTHLY_PERFORMANCE_SHEETS.forEach(item => {
            const url = (item.sheet_url || "").trim();
            if (!url || seen.has(url)) return;
            seen.add(url);
            source.push({
                name: item.sheet_name || item.month_label || "Bảng hiệu suất",
                url,
                team: "",
                month_label: item.month_label || "",
            });
        });

        // Fallback 1: employee monthly ads sheets (useful when performance history is empty)
        if (!source.length) {
            MONTHLY_SHEETS.forEach(item => {
                const url = (item.sheet_url || "").trim();
                if (!url || seen.has(url)) return;
                seen.add(url);
                source.push({
                    name: item.sheet_name || item.month_label || "Sheet đã dùng",
                    url,
                    team: "",
                    month_label: item.month_label || "",
                });
            });
        }

        // Fallback 2: accessible sheets list
        if (!source.length) {
            SHEETS.forEach(s => {
                const url = (s.url || "").trim();
                if (!url || seen.has(url)) return;
                seen.add(url);
                source.push({
                    name: s.name || "Sheet",
                    url,
                    team: s.team || "",
                    month_label: "",
                });
            });
        }
        return source;
    }

    // For ads sheet input or non-employee roles
    if (ROLE !== "employee") {
        return SHEETS.map(s => ({
            name: s.name || "",
            url: s.url || "",
            team: s.team || "",
            month_label: "",
        })).filter(s => s.url);
    }

    // Employee: suggest previously used ads sheets from monthly history.
    const seen = new Set();
    const source = [];
    MONTHLY_SHEETS.forEach(item => {
        const url = (item.sheet_url || "").trim();
        if (!url || seen.has(url)) return;
        seen.add(url);
        source.push({
            name: item.sheet_name || item.month_label || "Sheet đã dùng",
            url,
            team: "",
            month_label: item.month_label || "",
        });
    });
    return source;
}

function renderSuggestions(filterText = "") {
    const box = document.getElementById("sheetSuggestions");
    if (!box) return;
    const q    = filterText.trim().toLowerCase();
    const source = getSuggestionSource();
    const list = source.filter(s => !q || (s.name || "").toLowerCase().includes(q) || (s.url || "").toLowerCase().includes(q));
    if (!list.length) {
        box.innerHTML = '<div class="suggestion-empty">Không có gợi ý phù hợp</div>';
    } else {
        box.innerHTML = list.map(s => {
            const label = ROLE === "admin"
                ? `[${s.team || "?"}] ${s.name}`
                : (ROLE === "employee" && s.month_label ? `${s.name} · ${s.month_label}` : s.name);
            return `<button type="button" class="suggestion-item" data-url="${s.url.replace(/"/g,"&quot;")}">
                <span class="suggestion-name">${label}</span>
                <span class="suggestion-url">${s.url}</span>
            </button>`;
        }).join("");
    }
    box.querySelectorAll(".suggestion-item").forEach(item => {
        item.addEventListener("mousedown", () => {
            const targetInput = document.getElementById(activeSheetInputId);
            if (targetInput) {
                targetInput.value = item.getAttribute("data-url") || "";
            }
            box.style.display = "none";
            // Only fetch data if the ads sheet URL changed
            if (activeSheetInputId === "sheetUrl") {
                fetchAndRender(item.getAttribute("data-url"), false);
            }
        });
    });
}

// ─── Auto-fill toggle ─────────────────────────────────
async function loadAutoFillStatus() {
    try {
        const response = await fetch("/api/auto-fill-status");
        if (await handleSessionExpiredGateFromResponse(response)) return;
        const data = await response.json();
        if (data.success) { autoFillEnabled = !!data.enabled; renderAutoToggle(); }
    } catch (_) {}
}

async function toggleAutoFillStatus() {
    try {
        const response = await fetch("/api/auto-fill-status", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled: !autoFillEnabled }) });
        if (await handleSessionExpiredGateFromResponse(response)) return;
        const data = await response.json();
        if (data.success) { autoFillEnabled = !!data.enabled; renderAutoToggle(); showToast(autoFillEnabled ? "✅ Đã bật Auto Fill" : "⏸️ Đã tắt Auto Fill"); }
    } catch (_) {}
}

function renderAutoToggle() {
    const btn = document.getElementById("autoFillToggleBtn");
    if (!btn) return;
    btn.classList.remove("auto-on", "auto-off");
    btn.classList.add(autoFillEnabled ? "auto-on" : "auto-off");
    btn.textContent = autoFillEnabled ? "Auto Fill: ON" : "Auto Fill: OFF";
}

// ─── Save sheet ───────────────────────────────────────
async function saveSheetUrl(sheetUrl, performanceSheetUrl = "") {
    try {
        const response = await fetch("/api/save-sheet", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                sheet_url: sheetUrl,
                performance_sheet_url: performanceSheetUrl,
            }),
        });
        if (await handleSessionExpiredGateFromResponse(response)) return;
        const data = await response.json();
        if (!data.success) {
            const detail = buildSheetAccessHint(data);
            maybeAutoOpenSheetForAccess(data);
            showError("⚠️ " + (data.error || "Không lưu được link sheet") + (detail ? `\n${detail}` : ""));
            return;
        }

        if (data.success) {
            if (!data.already_exists) showToast("✅ " + data.message);
            if (ROLE === "employee" && data.month_key) {
                const idx = MONTHLY_SHEETS.findIndex(m => m.month_key === data.month_key);
                const item = {
                    month_key: data.month_key,
                    month_label: data.month_label || data.month_key,
                    sheet_name: data.name || "",
                    sheet_url: data.clean_url || sheetUrl,
                    folder_url: data.folder_url || `/monthly-folder/${data.month_key}`,
                };
                if (idx >= 0) {
                    MONTHLY_SHEETS[idx] = item;
                } else {
                    MONTHLY_SHEETS.unshift(item);
                }
                MONTHLY_SHEETS.sort((a, b) => (b.month_key || "").localeCompare(a.month_key || ""));
                populateMonthSelect();
                const sel = document.getElementById("monthSelect");
                if (sel) sel.value = data.month_key;
            }
        }
    } catch (_) {}
}

// ─── Helpers ──────────────────────────────────────────
function parseSpendJS(val) {
    return parseFloat(val.replace(/\./g,"").replace(/,/g,".").replace(/[^\d.]/g,"")) || 0;
}
function parseIntJS(val) {
    return parseInt(val.replace(/[^\d]/g,""), 10) || 0;
}
function formatCurrency(value) {
    return value.toLocaleString("vi-VN") + " VND";
}
function formatShortCurrency(value) {
    if (value >= 1_000_000) return (value/1_000_000).toFixed(1)+"M";
    if (value >= 1_000)     return (value/1_000).toFixed(0)+"K";
    return value.toFixed(0);
}
function escapeHtml(value) {
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/\"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
function showError(message) {
    const div = document.getElementById("errorMessage");
    div.textContent = message;
    div.style.whiteSpace = "pre-line";
    div.style.display = "block";
}

function buildSheetAccessHint(data) {
    if (!data) return "";

    if (Array.isArray(data.help_steps) && data.help_steps.length > 0) {
        return data.help_steps.map((step, idx) => `${idx + 1}. ${step}`).join("\n");
    }

    if (typeof data.help === "string" && data.help.trim()) {
        return data.help.trim();
    }

    return "";
}
function showToast(message) {
    let t = document.getElementById("toastNotification");
    if (!t) {
        t = document.createElement("div"); t.id = "toastNotification";
        t.style.cssText = "position:fixed;bottom:24px;right:24px;z-index:9999;background:#22c55e;color:white;padding:12px 20px;border-radius:8px;font-size:14px;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.3);transition:opacity 0.4s ease;";
        document.body.appendChild(t);
    }
    t.textContent = message; t.style.opacity = "1";
    clearTimeout(t._timeout); t._timeout = setTimeout(() => { t.style.opacity = "0"; }, 3500);
}

function setupInactivityLogout() {
    if (ROLE !== "employee") return;

    const activityEvents = ["click", "keydown", "mousedown", "mousemove", "scroll", "touchstart"];
    const onActivity = () => {
        resetInactivityTimer();
        void maybeSendSessionKeepAlive();
    };

    activityEvents.forEach((eventName) => {
        document.addEventListener(eventName, onActivity, { passive: true });
    });

    document.addEventListener("visibilitychange", () => {
        if (!document.hidden) onActivity();
    });
    window.addEventListener("focus", onActivity);
    onActivity();
}

function resetInactivityTimer() {
    clearTimeout(inactivityTimer);
    inactivityTimer = window.setTimeout(() => {
        showError("⚠️ Bạn đã không thao tác trong 10 phút. Hệ thống đang tự đăng xuất...");
        window.setTimeout(() => {
            window.location.href = "/logout?expired=1";
        }, 900);
    }, SESSION_TIMEOUT_MS);
}

async function maybeSendSessionKeepAlive() {
    const now = Date.now();
    if (document.hidden || now - lastKeepAliveAt < SESSION_KEEPALIVE_MS) {
        return;
    }

    lastKeepAliveAt = now;
    try {
        const response = await fetch("/api/session/ping", { method: "POST" });
        await handleSessionExpiredGateFromResponse(response);
    } catch (_) {
        // Ignore transient keepalive failures; the next real request will enforce auth.
    }
}

async function handleSessionExpiredGateFromResponse(response) {
    if (!response || response.status !== 401) {
        return false;
    }

    let data = {};
    try {
        data = await response.clone().json();
    } catch (_) {
        data = {};
    }
    return handleSessionExpiredGate(data, response.status);
}

function handleTelegramSetupGate(data, statusCode) {
    const setupUrl = (data && data.setup_url) ? data.setup_url : "";
    if (statusCode === 428 || setupUrl) {
        showError("⚠️ Bạn cần hoàn tất kết nối Telegram trước khi xem dashboard. Hệ thống đang chuyển đến màn kết nối...");
        setTimeout(() => {
            window.location.href = setupUrl || "/telegram/connect";
        }, 1200);
        return true;
    }
    return false;
}

function handleSessionExpiredGate(data, statusCode) {
    const loginUrl = (data && data.login_url) ? data.login_url : "/login?expired=1";
    if (statusCode === 401) {
        showError("⚠️ Phiên đăng nhập đã hết hạn. Hệ thống đang chuyển bạn về màn đăng nhập...");
        window.setTimeout(() => {
            window.location.href = loginUrl;
        }, 1200);
        return true;
    }
    return false;
}
