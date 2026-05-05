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
let _prevRows = []; // for diff highlight
let _hiddenCols = new Set(); // for column visibility
let _cmdActiveIdx = -1; // command palette selected index
let _activePreset = null; // date preset active chip

// ─── Init ─────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    setupInactivityLogout();
    initMetaApiHelperModal();
    populateMemberSelect();
    await loadSheetMemoryStatus();
    populateMonthSelect();
    await loadAutoFillStatus();
    initURLInputListeners();
    loadSheetConnectionStatus();

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

async function loadSheetMemoryStatus() {
    if (ROLE !== "employee") return;
    try {
        const response = await fetch("/api/sheet-memory/status");
        if (await handleSessionExpiredGateFromResponse(response)) return;
        const data = await response.json();
        if (!data || !data.success) return;

        if (Array.isArray(data.monthly_sheets)) {
            MONTHLY_SHEETS.length = 0;
            data.monthly_sheets.forEach(item => MONTHLY_SHEETS.push(item));
        }

        const sheetInput = document.getElementById("sheetUrl");
        if (sheetInput && !sheetInput.value && data.current_ads_sheet_url) {
            sheetInput.value = data.current_ads_sheet_url;
        }

        const perfInput = document.getElementById("performanceSheetUrl");
        if (perfInput && !perfInput.value && data.pinned_performance_sheet_url) {
            perfInput.value = data.pinned_performance_sheet_url;
        }
    } catch (_) {}
}

async function loadSheetConnectionStatus(showSpinner = false) {
    if (ROLE !== "employee") return;
    const panel = document.getElementById("sheetHealthPanel");
    const adsEl = document.getElementById("sheetHealthAds");
    const perfEl = document.getElementById("sheetHealthPerf");
    const recheckBtn = document.getElementById("btnRecheckSheet");
    if (!panel || !adsEl || !perfEl) return;

    if (showSpinner) {
        adsEl.className = "sheet-health-badge";
        adsEl.innerHTML = `<i class="fas fa-circle-notch fa-spin"></i> Đang kiểm tra...`;
        perfEl.className = "sheet-health-badge";
        perfEl.innerHTML = `<i class="fas fa-circle-notch fa-spin"></i> Đang kiểm tra...`;
        if (recheckBtn) { recheckBtn.disabled = true; recheckBtn.style.opacity = "0.4"; }
    }

    try {
        const res = await fetch("/api/sheet-connection-status");
        if (!res.ok) return;
        const data = await res.json();
        if (!data || !data.success) return;

        panel.style.display = "";

        const STALE_MS = 30 * 60 * 1000; // 30 minutes

        function renderBadge(el, info, label) {
            const ok = info && info.ok === true;
            const unknown = !info || info.ok == null;
            const name = (info && info.sheet_name) || label;
            const err = (info && info.error) || "";
            const checkedAt = info && info.checked_at ? new Date(info.checked_at) : null;
            const checkedStr = checkedAt
                ? checkedAt.toLocaleString("vi-VN", { timeZone: "Asia/Ho_Chi_Minh", hour: "2-digit", minute: "2-digit" })
                : "";
            const isStale = checkedAt && (Date.now() - checkedAt.getTime() > STALE_MS);
            const staleTag = isStale ? ` <span style="opacity:0.6;font-size:0.72rem">(cũ)</span>` : "";

            if (unknown) {
                el.className = "sheet-health-badge unknown";
                el.innerHTML = `<i class="fas fa-question-circle"></i> ${label}: chưa kiểm tra`;
            } else if (ok) {
                el.className = isStale ? "sheet-health-badge ok stale" : "sheet-health-badge ok";
                el.title = checkedStr ? `Kiểm tra lúc ${checkedStr}` : "";
                el.innerHTML = `<i class="fas fa-check-circle"></i> ${name || label}: OK${staleTag}`;
            } else {
                el.className = "sheet-health-badge error";
                el.title = err || "Lỗi không xác định";
                el.innerHTML = `<i class="fas fa-exclamation-circle"></i> ${label}: Mất kết nối${staleTag}`;
            }
        }

        renderBadge(adsEl, data.ads, "Sheet chi phí");
        renderBadge(perfEl, data.performance, "Sheet hiệu suất");

        // Hide performance badge if no performance sheet configured
        if (!data.performance || data.performance.ok == null) {
            perfEl.style.display = "none";
        } else {
            perfEl.style.display = "";
        }
    } catch (_) {}
    finally {
        if (recheckBtn) { recheckBtn.disabled = false; recheckBtn.style.opacity = ""; }
    }
}

async function recheckSheetHealth() {
    await loadSheetConnectionStatus(true);
}

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
        if ((!data.data || data.data.length === 0) && (!data.member_summaries || data.member_summaries.length === 0)) {
            showError("⚠️ Chưa có dữ liệu trong các sheet.");
            return;
        }

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

        const perfInputUrl = (document.getElementById("performanceSheetUrl")?.value || "").trim();
        const perfUrl = perfInputUrl || PERFORMANCE_SHEET_URL || SHEET_URL;
        if (perfUrl) {
            await loadPerformanceSummary(perfUrl);
        }

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
        renderProductRealtime();
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
            renderProductRealtime();
            const detail = buildSheetAccessHint(data);
            maybeAutoOpenSheetForAccess(data);
            if (data.error) {
                showError(`⚠️ ${data.error}${detail ? `\n${detail}` : ""}`);
            }
            return;
        }

        currentPerformanceMetrics = data.metrics || null;
        renderStats(filteredRows);
        renderProductRealtime();
    } catch (_) {
        currentPerformanceMetrics = null;
        renderStats(filteredRows);
        renderProductRealtime();
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
    const hasRows = Array.isArray(currentData.rows) && currentData.rows.length > 0;
    const hasRanking = Array.isArray(currentData.memberSummaries) && currentData.memberSummaries.length > 0;
    if (!hasRows && !hasRanking) { showError("Không có dữ liệu trong sheet"); return; }
    filteredRows = [...currentData.rows];
    currentPage = 1;
    const isAdminView = ROLE === "admin";
    document.getElementById("statsSection").style.display = "block";
    document.getElementById("tableSection").style.display = isAdminView ? "none" : "block";
    document.getElementById("chartsSection").style.display = isAdminView ? "none" : "grid";
    const rankSec = document.getElementById("rankingsSection");
    if (rankSec) rankSec.style.display = currentData.memberSummaries.length ? "block" : "none";
    const productSec = document.getElementById("productSection");
    if (productSec) productSec.style.display = "none";

    // Keep core data visible even if a secondary renderer (e.g. chart CDN) fails.
    try { renderStats(filteredRows); } catch (_) {}
    try { renderRankings(); } catch (_) {}
    try { renderProductRealtime(); } catch (_) {}
    if (!isAdminView) {
        try { renderTable(filteredRows); } catch (_) {}
        try { renderInsights(filteredRows); } catch (_) {
            showToast("⚠️ Không tải được khung tổng hợp. Dữ liệu bảng vẫn hiển thị bình thường.");
        }
    }
}

function renderStats(rows) {
    const profitability = currentData.profitability_metrics || null;
    const completionMetrics = currentPerformanceMetrics?.completion_percent || profitability?.completion_percent || null;
    const grossProfitMetrics = currentPerformanceMetrics?.gross_profit || profitability?.gross_profit || null;
    const grossProfitPercentMetrics = currentPerformanceMetrics?.gross_profit_percent || profitability?.gross_profit_percent || null;

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

function renderProductRealtime() {
    const section = document.getElementById("productSection");
    const tbody = document.getElementById("productRealtimeBody");
    const meta = document.getElementById("productRealtimeMeta");
    if (!section || !tbody || !meta) return;

    if (!(ROLE === "admin" || ROLE === "lead")) {
        section.style.display = "none";
        return;
    }

    const payload = currentPerformanceMetrics?.product_realtime || {};
    const items = Array.isArray(payload.items) ? [...payload.items] : [];
    items.sort((a, b) => Number(b.data_out || 0) - Number(a.data_out || 0));

    const fromDate = String(payload.date_from || "").trim();
    const toDate = String(payload.date_to || "").trim();
    if (fromDate || toDate) {
        meta.textContent = `Khoảng ngày đang áp dụng: ${fromDate || "—"} đến ${toDate || "—"}`;
        meta.style.display = "block";
    } else {
        meta.style.display = "none";
    }

    if (!items.length) {
        section.style.display = "block";
        tbody.innerHTML = '<tr><td colspan="8" class="account-status-empty">Chưa có dữ liệu sản phẩm realtime cho ngày hôm nay.</td></tr>';
        return;
    }

    tbody.innerHTML = items.map((item, idx) => {
        const dataOut = Number(item.data_out || 0);
        const revenue = Number(item.revenue || 0);
        const spend = Number(item.spend || 0);
        const ads = Number(item.ads_percent || 0);
        const stock = Number(item.stock || 0);
        const lng = Number(item.lng_percent || 0);
        return `<tr>
            <td>${idx + 1}</td>
            <td>${escapeHtml(item.name_vn || "—")}</td>
            <td>${Math.round(dataOut).toLocaleString("vi-VN")}</td>
            <td class="spend-cell">${formatCurrency(revenue)}</td>
            <td class="spend-cell">${formatCurrency(spend)}</td>
            <td>${ads.toLocaleString("vi-VN", { minimumFractionDigits: 0, maximumFractionDigits: 2 })}%</td>
            <td>${Math.round(stock).toLocaleString("vi-VN")}</td>
            <td>${lng.toLocaleString("vi-VN", { minimumFractionDigits: 0, maximumFractionDigits: 2 })}%</td>
        </tr>`;
    }).join("");

    section.style.display = "block";
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
    const COL_TOOLTIPS = {
        "Ngày": "Ngày chạy quảng cáo",
        "Tên tài khoản": "Tên tài khoản quảng cáo Meta/TikTok",
        "Tên sản phẩm - VN": "Tên sản phẩm trong chiến dịch",
        "Số Data": "Số lead / kết quả thu về (data khách hàng)",
        "Số tiền chi tiêu - VND": "Tổng chi phí quảng cáo theo VND trong ngày",
        "Số tiền chi tiêu - USD": "Tổng chi phí quảng cáo theo USD trong ngày",
        "Chi phí/KQ (USD)": "Chi phí trung bình để có 1 kết quả (CPR) tính bằng USD",
    };
    const headerHTML = headers.map(h => {
        const tip = COL_TOOLTIPS[h] ? ` title="${COL_TOOLTIPS[h]}" class="th-tip"` : "";
        return `<th${tip}>${h}</th>`;
    }).join("") + `<th title="${COL_TOOLTIPS[COMPUTED_COL]}" class="th-tip">${COMPUTED_COL}</th>`;
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
    const lng = currentData.profitability_metrics?.product_lng || currentPerformanceMetrics?.product_lng || {};
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

// B3: Auto-detect month from sheet URL hints when user types/pastes
function _autoDetectMonthFromUrl() {
    if (ROLE !== "employee") return;
    const sel = document.getElementById("monthSelect");
    if (!sel || sel.options.length <= 1) return;
    const val = (this.value || "").toLowerCase();
    const MONTH_NAMES_MAP = {
        "thang 1":1,"thang 2":2,"thang 3":3,"thang 4":4,"thang 5":5,"thang 6":6,
        "thang 7":7,"thang 8":8,"thang 9":9,"thang 10":10,"thang 11":11,"thang 12":12,
        "jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
        "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,
    };
    let detectedMonth = 0, detectedYear = new Date().getFullYear();
    for (const [name, m] of Object.entries(MONTH_NAMES_MAP)) {
        if (val.includes(name)) { detectedMonth = m; break; }
    }
    if (!detectedMonth) {
        const patterns = [
            /(\d{4})[_\-](\d{2})/,
            /t(\d{1,2})[_\-](\d{4})/i,
            /thang[_\- ]?(\d{1,2})/i,
        ];
        for (const p of patterns) {
            const m = val.match(p);
            if (m) {
                if (p.source.startsWith("(\\d{4})")) {
                    detectedYear = parseInt(m[1], 10);
                    detectedMonth = parseInt(m[2], 10);
                } else if (p.source.startsWith("t(\\d")) {
                    detectedMonth = parseInt(m[1], 10);
                    detectedYear = parseInt(m[2], 10);
                } else {
                    detectedMonth = parseInt(m[1], 10);
                }
                break;
            }
        }
    }
    if (!detectedMonth || detectedMonth < 1 || detectedMonth > 12) return;
    const mk = `${detectedYear}-${String(detectedMonth).padStart(2, "0")}`;
    const found = MONTHLY_SHEETS.find(s => s.month_key === mk);
    if (found && sel.value !== mk) {
        sel.value = mk;
        showToast(`📅 Tự động chọn tháng ${found.month_label || mk}`, 2500);
    }
}

// ─── URL suggestions ──────────────────────────────────
function initURLInputListeners() {
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
        // B3: Auto-detect month key from sheet name hint
        sheetInput.addEventListener("input", _autoDetectMonthFromUrl);
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
            // Show tab warning as a non-blocking notification
            if (data.tab_warning) {
                setTimeout(() => showToast("⚠️ " + data.tab_warning, 6000), 800);
            }
            const perfInput = document.getElementById("performanceSheetUrl");
            if (perfInput && !perfInput.value && data.pinned_performance_sheet_url) {
                perfInput.value = data.pinned_performance_sheet_url;
            }
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
function showToast(message, duration = 3500) {
    let t = document.getElementById("toastNotification");
    if (!t) {
        t = document.createElement("div"); t.id = "toastNotification";
        t.style.cssText = "position:fixed;bottom:24px;right:24px;z-index:9999;background:#22c55e;color:white;padding:12px 20px;border-radius:8px;font-size:14px;font-weight:500;box-shadow:0 4px 12px rgba(0,0,0,0.3);transition:opacity 0.4s ease;";
        document.body.appendChild(t);
    }
    t.textContent = message; t.style.opacity = "1";
    clearTimeout(t._timeout); t._timeout = setTimeout(() => { t.style.opacity = "0"; }, duration);
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

// ═══════════════════════════════════════════════════════
//  FEATURE ADDITIONS — G1 G2 G3 H1 I1 I2 I3 I4 J1 J2
//                      K1 K2 K3 L1 L2 L3 L5 M1
// ═══════════════════════════════════════════════════════

// ─── G1: Dark / Light mode ───────────────────────────
(function initDarkMode() {
    const DARK_KEY = "ads_dark_mode";
    const btn = document.getElementById("darkModeToggleBtn");
    const icon = document.getElementById("darkModeIcon");
    const label = document.getElementById("darkModeLabel");
    function apply(dark) {
        if (dark) {
            document.documentElement.classList.add("dark-mode");
        } else {
            document.documentElement.classList.remove("dark-mode");
        }
        if (icon) icon.className = dark ? "fas fa-sun" : "fas fa-moon";
        if (label) label.textContent = dark ? "Sáng" : "Tối";
        try { localStorage.setItem(DARK_KEY, dark ? "1" : "0"); } catch (_) {}
    }
    window.toggleDarkMode = function () {
        const isDark = document.documentElement.classList.contains("dark-mode");
        apply(!isDark);
    };
    // Restore preference or respect OS
    const stored = (() => { try { return localStorage.getItem(DARK_KEY); } catch (_) { return null; } })();
    if (stored === "1") apply(true);
    else if (stored === "0") apply(false);
    else if (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) apply(true);
})();

// ─── G2: Skeleton loading on stat cards ──────────────
function showStatSkeletons() {
    document.querySelectorAll(".stat-card").forEach(c => c.classList.add("loading"));
}
function hideStatSkeletons() {
    document.querySelectorAll(".stat-card").forEach(c => c.classList.remove("loading"));
}

// ─── G3: CountUp animation ───────────────────────────
function animateCount(el, targetStr) {
    if (!el) return;
    const num = parseFloat(String(targetStr).replace(/[^0-9.\-]/g, ""));
    if (isNaN(num) || num < 10) { el.classList.add("count-updated"); setTimeout(() => el.classList.remove("count-updated"), 600); return; }
    el.classList.remove("count-updated");
    void el.offsetWidth; // reflow
    el.classList.add("count-updated");
    setTimeout(() => el.classList.remove("count-updated"), 600);
}

// ─── H1: Sparkline SVG in stat cards ─────────────────
function renderSparklines(rows) {
    const spendData = {};
    rows.forEach(row => {
        const dateKey = (row["Ngày"] || "").trim();
        if (!dateKey) return;
        const spend = parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
        spendData[dateKey] = (spendData[dateKey] || 0) + spend;
    });
    const sortedKeys = Object.keys(spendData).sort((a, b) => {
        const pa = a.split("/"); const pb = b.split("/");
        const da = pa.length === 3 ? new Date(`${pa[2]}-${pa[1].padStart(2,"0")}-${pa[0].padStart(2,"0")}`) : new Date(a);
        const db = pb.length === 3 ? new Date(`${pb[2]}-${pb[1].padStart(2,"0")}-${pb[0].padStart(2,"0")}`) : new Date(b);
        return da - db;
    }).slice(-14);
    const vals = sortedKeys.map(k => spendData[k]);
    if (vals.length < 2) return;
    const max = Math.max(...vals) || 1;
    const W = 80, H = 24, pts = vals.map((v, i) => {
        const x = (i / (vals.length - 1)) * W;
        const y = H - (v / max) * (H - 4) - 2;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ");
    const svg = `<svg class="stat-sparkline" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}" xmlns="http://www.w3.org/2000/svg"><polyline points="${pts}" fill="none" stroke="#6366f1" stroke-width="1.5" stroke-linejoin="round" opacity="0.7"/></svg>`;
    const spendCard = document.getElementById("totalSpend");
    if (spendCard && spendCard.closest(".stat-card")) {
        const card = spendCard.closest(".stat-card");
        let existing = card.querySelector(".stat-sparkline");
        if (existing) existing.remove();
        card.insertAdjacentHTML("beforeend", svg);
    }
}

// ─── I1: Command palette ─────────────────────────────
const CMD_ITEMS = [
    { icon: "fa-rotate-right", label: "Tải lại dữ liệu", sub: "R", group: "Hành động", action: () => { const url = (document.getElementById("sheetUrl")?.value || "").trim(); if (url) fetchAndRender(url, false); } },
    { icon: "fa-filter", label: "Focus bộ lọc ngày", sub: "F", group: "Hành động", action: () => document.getElementById("dateFrom")?.focus() },
    { icon: "fa-table", label: "Cuộn xuống bảng dữ liệu", sub: "", group: "Điều hướng", action: () => document.getElementById("tableSection")?.scrollIntoView({behavior:"smooth"}) },
    { icon: "fa-chart-bar", label: "Cuộn xuống biểu đồ", sub: "", group: "Điều hướng", action: () => document.getElementById("chartsSection")?.scrollIntoView({behavior:"smooth"}) },
    { icon: "fa-moon", label: "Chuyển dark / light mode", sub: "", group: "Giao diện", action: () => window.toggleDarkMode() },
    { icon: "fa-file-csv", label: "Xuất bảng ra CSV", sub: "", group: "Xuất dữ liệu", action: () => exportTableCSV() },
    { icon: "fa-calendar-day", label: "Lọc: Hôm nay", sub: "", group: "Lọc nhanh", action: () => applyDatePreset("today") },
    { icon: "fa-calendar-week", label: "Lọc: 7 ngày gần nhất", sub: "", group: "Lọc nhanh", action: () => applyDatePreset("7d") },
    { icon: "fa-calendar", label: "Lọc: Tháng này", sub: "", group: "Lọc nhanh", action: () => applyDatePreset("month") },
    { icon: "fa-wand-magic-sparkles", label: "Mở AI chat", sub: "/", group: "Hành động", action: () => { const fab = document.querySelector(".ai-chat-fab"); if (fab) fab.click(); } },
    { icon: "fa-times", label: "Xóa bộ lọc", sub: "", group: "Hành động", action: () => resetDateFilter() },
];
// Dynamically add month items from MONTHLY_SHEETS
function getCmdItems() {
    const monthItems = (MONTHLY_SHEETS || []).slice(0, 12).map(s => ({
        icon: "fa-table-cells",
        label: `Tháng: ${s.month_label || s.month_key}`,
        sub: s.month_key,
        group: "Tháng",
        action: () => { const sel = document.getElementById("monthSelect"); if (sel) { sel.value = s.month_key; loadMonthSheet(s.month_key); } }
    }));
    const memberItems = (window.APP_SHEETS || []).slice(0, 10).map(s => ({
        icon: "fa-user",
        label: s.name || s.url,
        sub: s.team || "",
        group: "Thành viên",
        action: () => { const sel = document.getElementById("memberSelect"); if (sel) { sel.value = s.url; loadMemberSheet(s.url); } }
    }));
    return [...CMD_ITEMS, ...monthItems, ...memberItems];
}
function openCmdPalette() {
    const overlay = document.getElementById("cmdPaletteOverlay");
    if (!overlay) return;
    overlay.style.display = "flex";
    const inp = document.getElementById("cmdInput");
    if (inp) { inp.value = ""; inp.focus(); }
    _cmdActiveIdx = -1;
    renderCmdResults();
}
function closeCmdPalette(e) {
    if (e && e.target !== document.getElementById("cmdPaletteOverlay")) return;
    const overlay = document.getElementById("cmdPaletteOverlay");
    if (overlay) overlay.style.display = "none";
}
function renderCmdResults() {
    const q = (document.getElementById("cmdInput")?.value || "").toLowerCase().trim();
    const items = getCmdItems();
    const filtered = q ? items.filter(it => it.label.toLowerCase().includes(q) || (it.group || "").toLowerCase().includes(q)) : items;
    const container = document.getElementById("cmdResults");
    if (!container) return;
    _cmdActiveIdx = -1;
    if (!filtered.length) { container.innerHTML = '<div class="cmd-group-title">Không tìm thấy kết quả</div>'; return; }
    const groups = {};
    filtered.forEach(it => {
        if (!groups[it.group]) groups[it.group] = [];
        groups[it.group].push(it);
    });
    let html = "";
    let globalIdx = 0;
    Object.entries(groups).forEach(([group, its]) => {
        html += `<div class="cmd-group-title">${group}</div>`;
        its.forEach(it => {
            html += `<div class="cmd-result-item" data-idx="${globalIdx++}" onclick="executeCmdItem(${items.indexOf(it)})">
                <i class="fas ${it.icon}"></i>${escapeHtml(it.label)}
                ${it.sub ? `<span class="cmd-result-sub">${escapeHtml(it.sub)}</span>` : ""}
            </div>`;
        });
    });
    container.innerHTML = html;
}
function executeCmdItem(idx) {
    const items = getCmdItems();
    const it = items[idx];
    const overlay = document.getElementById("cmdPaletteOverlay");
    if (overlay) overlay.style.display = "none";
    if (it && it.action) setTimeout(it.action, 50);
}

// ─── I2: Date range presets ───────────────────────────
function applyDatePreset(preset) {
    const from = document.getElementById("dateFrom");
    const to   = document.getElementById("dateTo");
    if (!from || !to) return;
    const now = new Date();
    const fmt = d => d.toISOString().slice(0, 10);
    document.querySelectorAll(".date-chip").forEach(c => c.classList.remove("active"));
    const active = document.querySelector(`.date-chip[onclick*="'${preset}'"]`);
    if (active) active.classList.add("active");
    _activePreset = preset;
    if (preset === "today") {
        from.value = fmt(now); to.value = fmt(now);
    } else if (preset === "yesterday") {
        const y = new Date(now); y.setDate(y.getDate() - 1);
        from.value = fmt(y); to.value = fmt(y);
    } else if (preset === "7d") {
        const s = new Date(now); s.setDate(s.getDate() - 6);
        from.value = fmt(s); to.value = fmt(now);
    } else if (preset === "month") {
        from.value = fmt(new Date(now.getFullYear(), now.getMonth(), 1));
        to.value = fmt(now);
    }
    applyDateFilter();
}

// ─── I3: Saved filter presets ────────────────────────
const SAVED_FILTER_KEY = "ads_saved_filters_v1";
function loadSavedFilters() {
    try { return JSON.parse(localStorage.getItem(SAVED_FILTER_KEY) || "[]"); } catch (_) { return []; }
}
function persistSavedFilters(list) {
    try { localStorage.setItem(SAVED_FILTER_KEY, JSON.stringify(list)); } catch (_) {}
}
function renderSavedFilterBar() {
    const bar = document.getElementById("savedFiltersBar");
    if (!bar) return;
    const filters = loadSavedFilters();
    if (!filters.length) { bar.style.display = "none"; return; }
    bar.style.display = "flex";
    bar.innerHTML = filters.map((f, i) =>
        `<span class="sf-chip" onclick="applySavedFilter(${i})">${escapeHtml(f.label)}<span class="sf-del" onclick="deleteSavedFilter(event,${i})">×</span></span>`
    ).join("");
}
function saveCurrentFilter() {
    const from = document.getElementById("dateFrom")?.value || "";
    const to   = document.getElementById("dateTo")?.value || "";
    if (!from && !to) { showToast("⚠️ Chưa chọn khoảng ngày để lưu"); return; }
    const label = from === to ? from : `${from || "—"} → ${to || "—"}`;
    const name = window.prompt("Đặt tên cho bộ lọc:", label);
    if (!name) return;
    const filters = loadSavedFilters();
    filters.push({ label: name.trim(), from, to });
    persistSavedFilters(filters);
    renderSavedFilterBar();
    showToast("✅ Đã lưu bộ lọc: " + name.trim());
}
function applySavedFilter(idx) {
    const f = loadSavedFilters()[idx];
    if (!f) return;
    const from = document.getElementById("dateFrom");
    const to   = document.getElementById("dateTo");
    if (from) from.value = f.from;
    if (to)   to.value   = f.to;
    applyDateFilter();
}
function deleteSavedFilter(e, idx) {
    e.stopPropagation();
    const filters = loadSavedFilters();
    filters.splice(idx, 1);
    persistSavedFilters(filters);
    renderSavedFilterBar();
}
// Load saved filters on page ready
document.addEventListener("DOMContentLoaded", () => renderSavedFilterBar());

// ─── I4: Column visibility ────────────────────────────
function toggleColVisPanel() {
    const panel = document.getElementById("colVisPanel");
    if (!panel) return;
    panel.style.display = panel.style.display === "none" ? "block" : "none";
    if (panel.style.display === "block") buildColVisCheckboxes();
}
function buildColVisCheckboxes() {
    const container = document.getElementById("colVisCheckboxes");
    if (!container) return;
    const headers = (currentData.headers || []);
    const allCols = [...headers, "Chi phí/KQ (USD)"];
    container.innerHTML = allCols.map(h =>
        `<label><input type="checkbox" ${_hiddenCols.has(h) ? "" : "checked"} onchange="toggleCol('${h.replace(/'/g,"\\'")}', this.checked)">${escapeHtml(h)}</label>`
    ).join("");
}
function toggleCol(colName, visible) {
    if (visible) { _hiddenCols.delete(colName); }
    else          { _hiddenCols.add(colName); }
    applyColVisibility();
}
function applyColVisibility() {
    const table = document.getElementById("dataTable");
    if (!table) return;
    const headers = (currentData.headers || []);
    const allCols = [...headers, "Chi phí/KQ (USD)"];
    allCols.forEach((h, i) => {
        const display = _hiddenCols.has(h) ? "none" : "";
        const ths = table.querySelectorAll(`thead tr th:nth-child(${i + 1})`);
        const tds = table.querySelectorAll(`tbody tr td:nth-child(${i + 1})`);
        ths.forEach(el => el.style.display = display);
        tds.forEach(el => el.style.display = display);
    });
}
function resetColVis() {
    _hiddenCols.clear();
    buildColVisCheckboxes();
    applyColVisibility();
}

// ─── J1: Export CSV ───────────────────────────────────
function exportTableCSV() {
    const rows = filteredRows;
    if (!rows || !rows.length) { showToast("⚠️ Không có dữ liệu để xuất"); return; }
    const headers = currentData.headers || [];
    const allCols = [...headers, "Chi phí/KQ (USD)"];
    const visible = allCols.filter(h => !_hiddenCols.has(h));
    const escape = v => `"${String(v || "").replace(/"/g, '""')}"`;
    const lines = [visible.map(escape).join(",")];
    rows.forEach(row => {
        const usdRaw = row["Số tiền chi tiêu - USD"] || "";
        const usdVal = parseSpendJS(usdRaw);
        const dataVal = parseInt(row["Số Data"] || "0", 10) || 0;
        const cpr = (usdVal > 0 && dataVal > 0) ? (usdVal / dataVal).toFixed(3) : "";
        const cells = visible.map(h => h === "Chi phí/KQ (USD)" ? escape(cpr) : escape(row[h] || ""));
        lines.push(cells.join(","));
    });
    const bom = "\uFEFF";
    const blob = new Blob([bom + lines.join("\r\n")], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `ads_data_${new Date().toISOString().slice(0,10)}.csv`;
    document.body.appendChild(a); a.click();
    document.body.removeChild(a); URL.revokeObjectURL(url);
    showToast(`✅ Đã xuất ${rows.length} dòng ra CSV`);
}

// ─── K1: Budget alert thresholds ─────────────────────
const BUDGET_ALERT_KEY = "ads_budget_alerts_v1";
function loadBudgetAlerts() {
    try { return JSON.parse(localStorage.getItem(BUDGET_ALERT_KEY) || "{}"); } catch (_) { return {}; }
}
function toggleBudgetPanel() {
    const panel = document.getElementById("budgetAlertPanel");
    if (!panel) return;
    panel.style.display = panel.style.display === "none" ? "block" : "none";
    if (panel.style.display === "block") {
        const alerts = loadBudgetAlerts();
        const cprEl = document.getElementById("alertCprThreshold");
        const adsEl = document.getElementById("alertAdsThreshold");
        if (cprEl) cprEl.value = alerts.cpr_threshold || "";
        if (adsEl) adsEl.value = alerts.ads_threshold || "";
    }
}
function saveBudgetAlerts() {
    const cprEl = document.getElementById("alertCprThreshold");
    const adsEl = document.getElementById("alertAdsThreshold");
    const data = {
        cpr_threshold: parseFloat(cprEl?.value || "0") || 0,
        ads_threshold: parseFloat(adsEl?.value || "0") || 0,
    };
    try { localStorage.setItem(BUDGET_ALERT_KEY, JSON.stringify(data)); } catch (_) {}
    const statusEl = document.getElementById("budgetAlertStatus");
    if (statusEl) statusEl.textContent = "✅ Đã lưu!";
    setTimeout(() => { if (statusEl) statusEl.textContent = ""; }, 2000);
    showToast("✅ Đã lưu ngưỡng cảnh báo");
    checkBudgetAlerts();
}
function checkBudgetAlerts() {
    const alerts = loadBudgetAlerts();
    if (!alerts.cpr_threshold && !alerts.ads_threshold) return;
    if (!currentData.rows.length) return;
    const today = new Date();
    const todayKey = `${String(today.getDate()).padStart(2,"0")}/${String(today.getMonth()+1).padStart(2,"0")}/${today.getFullYear()}`;
    let todaySpend = 0, todayData = 0;
    currentData.rows.filter(r => (r["Ngày"]||"").trim() === todayKey).forEach(r => {
        todaySpend += parseSpendJS(r["Số tiền chi tiêu - VND"] || "");
        todayData  += parseInt(r["Số Data"] || "0", 10) || 0;
    });
    const cpr = todayData > 0 ? Math.round(todaySpend / todayData) : 0;
    if (alerts.cpr_threshold && cpr > 0 && cpr > alerts.cpr_threshold) {
        showToast(`🔔 Cảnh báo! Chi phí/data hôm nay: ${cpr.toLocaleString("vi-VN")} VND > ngưỡng ${alerts.cpr_threshold.toLocaleString("vi-VN")}`, 7000);
    }
    const adsPercent = parseFloat((currentData.ads_percent || "0").replace(",", ".")) || 0;
    if (alerts.ads_threshold && adsPercent > 0 && adsPercent > alerts.ads_threshold) {
        showToast(`🔔 Cảnh báo! % Ads: ${adsPercent}% > ngưỡng ${alerts.ads_threshold}%`, 7000);
    }
}

// ─── K2: Anomaly detection ────────────────────────────
function detectAnomalies(rows) {
    if (!rows || rows.length < 4) return;
    const spends = rows.map(r => parseSpendJS(r["Số tiền chi tiêu - VND"] || "")).filter(v => v > 0);
    if (spends.length < 4) return;
    const mean = spends.reduce((a, b) => a + b, 0) / spends.length;
    const threshold = mean * 2.2;
    const tbody = document.getElementById("tableBody");
    if (!tbody) return;
    const trs = tbody.querySelectorAll("tr");
    rows.forEach((row, i) => {
        const spend = parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
        if (trs[i] && spend > threshold) {
            trs[i].classList.add("anomaly-row");
            trs[i].title = `⚠️ Chi tiêu bất thường: ${spend.toLocaleString("vi-VN")} VND (trung bình: ${Math.round(mean).toLocaleString("vi-VN")} VND)`;
        } else if (trs[i]) {
            trs[i].classList.remove("anomaly-row");
        }
    });
}

// ─── K3: Browser push notifications ──────────────────
(function initPushNotify() {
    const btn = document.getElementById("pushNotifyBtn");
    if (!("Notification" in window)) return;
    if (btn) btn.style.display = "";
    function updatePushBtnState() {
        if (!btn) return;
        if (Notification.permission === "granted") {
            btn.title = "Thông báo đã bật";
            btn.style.opacity = "1";
            btn.querySelector("i").className = "fas fa-bell";
        } else {
            btn.title = "Bật thông báo trình duyệt";
            btn.querySelector("i").className = "fas fa-bell-slash";
        }
    }
    updatePushBtnState();
    window.requestPushPermission = async function () {
        if (Notification.permission === "granted") {
            new Notification("Chi Phí Ads Dashboard", {
                body: "Thông báo đã được bật rồi đại ca ơi!",
                icon: "/static/favicon.ico",
            });
            return;
        }
        const result = await Notification.requestPermission();
        updatePushBtnState();
        if (result === "granted") {
            new Notification("Chi Phí Ads Dashboard", {
                body: "✅ Đã bật thông báo! Em sẽ báo khi có cảnh báo ngân sách.",
                icon: "/static/favicon.ico",
            });
            showToast("✅ Đã bật thông báo trình duyệt");
        } else {
            showToast("⚠️ Trình duyệt từ chối quyền thông báo");
        }
    };
    window._sendPushNotification = function (title, body) {
        if (Notification.permission !== "granted") return;
        new Notification(title || "Chi Phí Ads", { body: body || "", icon: "/static/favicon.ico" });
    };
})();

// ─── L1: Onboarding checklist ─────────────────────────
(function initOnboarding() {
    const DISMISS_KEY = "ads_onboarding_dismissed_v1";
    const dismissed = (() => { try { return localStorage.getItem(DISMISS_KEY) === "1"; } catch (_) { return false; } })();
    if (dismissed) return;
    const steps = [
        { label: "Nhập link Google Sheet báo cáo", done: () => !!(document.getElementById("sheetUrl")?.value || SHEET_URL) },
        { label: "Kết nối Telegram để nhận báo cáo", done: () => false }, // server-side check not accessible
        { label: "Đọc dữ liệu lần đầu", done: () => currentData.rows.length > 0 },
        { label: "Khám phá AI trợ lý (bấm nút ✨)", done: () => false },
    ];
    function render() {
        const panel = document.getElementById("onboardingPanel");
        const list = document.getElementById("onboardingList");
        if (!panel || !list) return;
        const allDone = steps.every(s => s.done());
        if (allDone) { panel.style.display = "none"; return; }
        list.innerHTML = steps.map(s => {
            const done = s.done();
            return `<li class="${done ? "done" : ""}">${escapeHtml(s.label)}</li>`;
        }).join("");
        panel.style.display = "block";
    }
    window.dismissOnboarding = function () {
        try { localStorage.setItem(DISMISS_KEY, "1"); } catch (_) {}
        const panel = document.getElementById("onboardingPanel");
        if (panel) panel.style.display = "none";
    };
    // Delay render slightly so currentData may be available
    setTimeout(render, 500);
    // Re-render on data load
    const _origRender = window.renderData;
    window.renderData = function () {
        if (_origRender) _origRender.call(this, ...arguments);
        setTimeout(render, 300);
    };
})();

// ─── L2: Keyboard shortcuts ───────────────────────────
(function initKeyboardShortcuts() {
    document.addEventListener("keydown", e => {
        const tag = (document.activeElement?.tagName || "").toLowerCase();
        const inInput = tag === "input" || tag === "textarea" || tag === "select" || document.activeElement?.isContentEditable;
        // Ctrl+K → command palette
        if ((e.ctrlKey || e.metaKey) && e.key === "k") {
            e.preventDefault();
            const overlay = document.getElementById("cmdPaletteOverlay");
            if (overlay?.style.display === "flex") closeCmdPalette(); else openCmdPalette();
            return;
        }
        // Escape → close command palette
        if (e.key === "Escape") {
            const overlay = document.getElementById("cmdPaletteOverlay");
            if (overlay?.style.display === "flex") { overlay.style.display = "none"; return; }
        }
        // Arrow keys in command palette
        if (document.getElementById("cmdPaletteOverlay")?.style.display === "flex") {
            const items = document.querySelectorAll(".cmd-result-item");
            if (e.key === "ArrowDown") {
                e.preventDefault();
                _cmdActiveIdx = Math.min(_cmdActiveIdx + 1, items.length - 1);
                items.forEach((el, i) => el.classList.toggle("active", i === _cmdActiveIdx));
                items[_cmdActiveIdx]?.scrollIntoView({ block: "nearest" });
                return;
            }
            if (e.key === "ArrowUp") {
                e.preventDefault();
                _cmdActiveIdx = Math.max(_cmdActiveIdx - 1, 0);
                items.forEach((el, i) => el.classList.toggle("active", i === _cmdActiveIdx));
                items[_cmdActiveIdx]?.scrollIntoView({ block: "nearest" });
                return;
            }
            if (e.key === "Enter" && _cmdActiveIdx >= 0) {
                e.preventDefault();
                items[_cmdActiveIdx]?.click();
                return;
            }
        }
        if (inInput) return; // don't intercept normal typing
        // R → reload
        if (e.key === "r" || e.key === "R") {
            const url = (document.getElementById("sheetUrl")?.value || "").trim();
            if (url) { e.preventDefault(); showToast("🔄 Đang tải lại..."); fetchAndRender(url, false); }
        }
        // F → focus date from
        if (e.key === "f" || e.key === "F") {
            const el = document.getElementById("dateFrom");
            if (el) { e.preventDefault(); el.focus(); }
        }
        // / → open AI chat
        if (e.key === "/") {
            e.preventDefault();
            const fab = document.querySelector(".ai-chat-fab");
            if (fab) fab.click();
        }
        // T → scroll to table
        if (e.key === "t" || e.key === "T") {
            document.getElementById("tableSection")?.scrollIntoView({ behavior: "smooth" });
        }
    });
    // Show hint toast once
    const HINT_KEY = "ads_kbd_hint_shown";
    if (!localStorage.getItem(HINT_KEY)) {
        setTimeout(() => {
            showToast("⌨️ Phím tắt: Ctrl+K = tìm kiếm · R = reload · / = AI chat", 5000);
            try { localStorage.setItem(HINT_KEY, "1"); } catch (_) {}
        }, 3000);
    }
})();

// ─── L3: Diff highlight changed rows ──────────────────
function diffAndHighlight(newRows) {
    const prevMap = {};
    _prevRows.forEach(r => {
        const key = `${r["Ngày"]}|${r["Tên tài khoản"]}`;
        prevMap[key] = r["Số tiền chi tiêu - VND"] || "";
    });
    const tbody = document.getElementById("tableBody");
    if (!tbody) { _prevRows = [...newRows]; return; }
    const trs = tbody.querySelectorAll("tr");
    newRows.forEach((row, i) => {
        const key = `${row["Ngày"]}|${row["Tên tài khoản"]}`;
        const prevVal = prevMap[key];
        const curVal  = row["Số tiền chi tiêu - VND"] || "";
        if (prevVal !== undefined && prevVal !== curVal && trs[i]) {
            trs[i].classList.remove("row-changed");
            void trs[i].offsetWidth; // reflow to restart animation
            trs[i].classList.add("row-changed");
            setTimeout(() => trs[i]?.classList.remove("row-changed"), 2200);
        }
    });
    _prevRows = [...newRows];
}

// ─── L5: Multi-tab BroadcastChannel sync ─────────────
(function initTabSync() {
    if (!("BroadcastChannel" in window)) return;
    const ch = new BroadcastChannel("ads_dashboard_sync");
    ch.addEventListener("message", e => {
        if (!e.data) return;
        if (e.data.type === "data_reload") {
            showToast("🔄 Tab khác đã tải lại dữ liệu mới", 2500);
        }
        if (e.data.type === "dark_mode") {
            const isDark = e.data.dark;
            if (isDark) document.documentElement.classList.add("dark-mode");
            else        document.documentElement.classList.remove("dark-mode");
        }
    });
    window._broadcastDataReload = () => { try { ch.postMessage({ type: "data_reload" }); } catch (_) {} };
    // patch toggleDarkMode to broadcast
    const _origToggle = window.toggleDarkMode;
    window.toggleDarkMode = function () {
        if (_origToggle) _origToggle();
        const isDark = document.documentElement.classList.contains("dark-mode");
        try { ch.postMessage({ type: "dark_mode", dark: isDark }); } catch (_) {}
    };
})();

// ─── Patch renderData/renderTable to call new features ─
(function patchRenderHooks() {
    const _origRenderTable = window.renderTable;
    window.renderTable = function (rows) {
        if (_origRenderTable) _origRenderTable.call(this, rows);
        // Apply col visibility after render
        try { applyColVisibility(); } catch (_) {}
        // Diff highlight
        try { diffAndHighlight(rows); } catch (_) {}
        // Anomaly detection
        try { detectAnomalies(rows); } catch (_) {}
    };
    const _origRenderStatsOuter = window.renderStats;
    window.renderStats = function (rows) {
        hideStatSkeletons();
        if (_origRenderStatsOuter) _origRenderStatsOuter.call(this, rows);
        // Sparklines
        try { renderSparklines(rows || currentData.rows || []); } catch (_) {}
        // Budget alerts
        try { checkBudgetAlerts(); } catch (_) {}
        // CountUp flash on stat values
        try {
            document.querySelectorAll(".stat-value").forEach(el => {
                animateCount(el, el.textContent);
            });
        } catch (_) {}
        // Broadcast to other tabs
        try { if (window._broadcastDataReload) _broadcastDataReload(); } catch (_) {}
    };
    // Show skeletons when fetch starts
    const _origFetch = window.fetchAndRender;
    window.fetchAndRender = function () {
        showStatSkeletons();
        return _origFetch ? _origFetch.apply(this, arguments) : Promise.resolve();
    };
})();

