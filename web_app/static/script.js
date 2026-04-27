// ═══════════════════════════════════════════════════════
//  Chi Phí Ads Dashboard — script.js
//  Supports 3 roles: admin | lead | employee
// ═══════════════════════════════════════════════════════

const ROLE      = window.APP_ROLE      || "employee";
const TEAM      = window.APP_TEAM      || "";
const DISPLAY   = window.APP_DISPLAY   || "";
const SHEET_URL = window.APP_SHEET_URL || "";
const SHEETS    = window.APP_SHEETS    || [];
const MONTHLY_SHEETS = window.APP_MONTHLY_SHEETS || [];

// ─── State ────────────────────────────────────────────
let currentData  = { rows: [], headers: [], ads_percent: "", memberSummaries: [] };
let filteredRows = [];
let autoFillEnabled = true;
let charts = { spendByDate: null, spendByProduct: null };
let currentPage = 1;
let pageSize = 50;

// ─── Init ─────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    populateMemberSelect();
    populateMonthSelect();
    await loadAutoFillStatus();
    initURLInputListeners();

    if (ROLE === "employee") {
        const defaultSheet = SHEET_URL || (SHEETS[0] && SHEETS[0].url) || "";
        const sheetInput = document.getElementById("sheetUrl");
        if (sheetInput && defaultSheet) sheetInput.value = defaultSheet;
        const sel = document.getElementById("memberSelect");
        if (sel && defaultSheet) sel.value = defaultSheet;

        if (defaultSheet) {
            await fetchAndRender(defaultSheet, false);
        } else {
            showError("⚠️ Tài khoản chưa được gán sheet. Liên hệ Admin.");
        }
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

// ─── Load ALL sheets (lead/admin) ─────────────────────
async function loadAllData() {
    const btn = document.getElementById("btnLoadAll");
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Đang tải...'; }
    const spinner = document.getElementById("loadingSpinner");
    if (spinner) spinner.style.display = "block";
    document.getElementById("errorMessage").style.display = "none";

    try {
        const res  = await fetch("/api/fetch-all-data", { method: "POST" });
        const data = await res.json();
        if (spinner) spinner.style.display = "none";
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-layer-group"></i> Tải báo cáo tổng'; }

        if (handleTelegramSetupGate(data, res.status)) return;

        if (!data.success) { showError("❌ " + (data.error || "Không thể tải dữ liệu tổng")); return; }
        if (!data.data || data.data.length === 0) { showError("⚠️ Chưa có dữ liệu trong các sheet."); return; }

        currentData = { rows: data.data, headers: data.headers, ads_percent: "", memberSummaries: data.member_summaries || [] };
        filteredRows = [...currentData.rows];
        resetDateInputs();
        renderData();

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
        const res  = await fetch("/api/fetch-data", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sheet_url: sheetUrl }) });
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
        if (!data.data || data.data.length === 0) { showError("⚠️ Sheet không có dữ liệu trong tab 'Chi phí ADS'"); return; }

        currentData  = { rows: data.data, headers: data.headers, ads_percent: data.ads_percent || "", memberSummaries: [] };
        filteredRows = [...currentData.rows];
        resetDateInputs();
        renderData();

        if (shouldAutoSave) saveSheetUrl(sheetUrl);
    } catch (e) {
        if (spinner) spinner.style.display = "none";
        showError("❌ Lỗi kết nối: " + e.message);
    }
}

async function handleSubmit(event) {
    event.preventDefault();
    await fetchAndRender(document.getElementById("sheetUrl").value.trim(), true);
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
    renderCharts(filteredRows);
}

function resetDateFilter() {
    filteredRows = [...currentData.rows];
    currentPage = 1;
    resetDateInputs();
    const info = document.getElementById("filterInfo");
    if (info) info.style.display = "none";
    renderStats(filteredRows);
    renderTable(filteredRows);
    renderCharts(filteredRows);
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
    try { renderCharts(filteredRows); } catch (_) {
        const chartSection = document.getElementById("chartsSection");
        if (chartSection) chartSection.style.display = "none";
        showToast("⚠️ Không tải được biểu đồ. Dữ liệu bảng vẫn hiển thị bình thường.");
    }
}

function renderStats(rows) {
    let totalSpend = 0, totalResults = 0;
    rows.forEach(row => {
        totalSpend   += parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
        totalResults += parseIntJS(row["Số Data"] || "");
    });
    const costPerResult = totalResults > 0 ? Math.round(totalSpend / totalResults) : 0;
    setStatValue("totalSpend", totalSpend.toLocaleString("vi-VN"), "VND");
    setStatValue("totalResults", totalResults.toLocaleString("en-US"), "data");
    setStatValue("costPerResult", costPerResult.toLocaleString("vi-VN"), "VND");

    const adsRaw = (currentData.ads_percent || "").toString().trim();
    if (!adsRaw || adsRaw === "—") {
        setStatValue("adsPercent", "—", "");
    } else {
        const adsMain = adsRaw.endsWith("%") ? adsRaw.slice(0, -1) : adsRaw;
        setStatValue("adsPercent", adsMain, "%");
    }
}

function setStatValue(elementId, mainValue, unit = "") {
    const el = document.getElementById(elementId);
    if (!el) return;
    const safeMain = escapeHtml(mainValue || "—");
    const safeUnit = escapeHtml(unit || "");

    if (!safeUnit) {
        el.innerHTML = `<span class="value-main">${safeMain}</span>`;
        return;
    }

    el.innerHTML = `<span class="value-main">${safeMain}</span><span class="value-unit">${safeUnit}</span>`;
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

function renderTable(rows) {
    const headers = currentData.headers;
    const totalRows = rows.length;
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));
    if (currentPage > totalPages) currentPage = totalPages;
    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;
    const pageRows = rows.slice(start, end);

    document.getElementById("tableHeader").innerHTML = headers.map(h => `<th>${h}</th>`).join("");
    document.getElementById("tableBody").innerHTML   = pageRows.map(row =>
        "<tr>" + headers.map(h => `<td>${row[h] || "-"}</td>`).join("") + "</tr>"
    ).join("");

    updatePagination(totalRows, totalPages, start, pageRows.length);
}

function updatePagination(totalRows, totalPages, startIndex, currentCount) {
    const pageInfo = document.getElementById("pageInfo");
    const prevBtn = document.getElementById("prevPageBtn");
    const nextBtn = document.getElementById("nextPageBtn");
    const countInfo = document.getElementById("tableCountInfo");

    if (pageInfo) pageInfo.textContent = `Trang ${currentPage}/${totalPages}`;
    if (prevBtn) prevBtn.disabled = currentPage <= 1;
    if (nextBtn) nextBtn.disabled = currentPage >= totalPages;
    if (countInfo) {
        const from = totalRows === 0 ? 0 : startIndex + 1;
        const to = startIndex + currentCount;
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

function renderCharts(rows) {
    if (typeof Chart === "undefined") {
        throw new Error("Chart.js not available");
    }

    const dateCanvas = document.getElementById("spendByDateChart");
    const productCanvas = document.getElementById("spendByProductChart");
    if (!dateCanvas || !productCanvas) {
        throw new Error("Chart canvas missing");
    }

    // Date chart
    const spendByDate = {};
    rows.forEach(row => {
        const d = row["Ngày"] || "Unknown";
        spendByDate[d] = (spendByDate[d] || 0) + parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
    });
    const dateLabels = Object.keys(spendByDate).sort((a, b) => {
        const da = parseViDate(a), db = parseViDate(b);
        return (da && db) ? da - db : a.localeCompare(b);
    });
    const ctx1 = dateCanvas.getContext("2d");
    if (charts.spendByDate) charts.spendByDate.destroy();
    charts.spendByDate = new Chart(ctx1, {
        type: "line",
        data: { labels: dateLabels, datasets: [{ label: "Chi Phí (VND)", data: dateLabels.map(d => spendByDate[d]), borderColor: "#3498db", backgroundColor: "rgba(52,152,219,0.1)", borderWidth: 2, fill: true, tension: 0.4, pointRadius: 5, pointBackgroundColor: "#3498db", pointBorderColor: "#fff", pointBorderWidth: 2 }] },
        options: { responsive: true, maintainAspectRatio: true, plugins: { legend: { display: true, position: "top" } }, scales: { y: { beginAtZero: true, ticks: { callback: v => formatShortCurrency(v) } } } },
    });

    // Product chart
    const spendByProduct = {};
    rows.forEach(row => {
        const p = row["Tên sản phẩm - VN"] || "Unknown";
        spendByProduct[p] = (spendByProduct[p] || 0) + parseSpendJS(row["Số tiền chi tiêu - VND"] || "");
    });
    const pLabels = Object.keys(spendByProduct);
    const colors  = ["#3498db","#e74c3c","#2ecc71","#f39c12","#9b59b6","#1abc9c","#34495e","#e67e22","#c0392b","#27ae60"];
    const ctx2 = productCanvas.getContext("2d");
    if (charts.spendByProduct) charts.spendByProduct.destroy();
    charts.spendByProduct = new Chart(ctx2, {
        type: "doughnut",
        data: { labels: pLabels, datasets: [{ data: pLabels.map(p => spendByProduct[p]), backgroundColor: colors.slice(0, pLabels.length), borderColor: "#fff", borderWidth: 2 }] },
        options: { responsive: true, maintainAspectRatio: true, plugins: { legend: { display: true, position: "bottom" } } },
    });
}

// ─── URL suggestions ──────────────────────────────────
function initURLInputListeners() {
    const input   = document.getElementById("sheetUrl");
    const box     = document.getElementById("sheetSuggestions");
    const wrap    = document.querySelector(".input-wrap");
    const autoBtn = document.getElementById("autoFillToggleBtn");
    if (autoBtn) autoBtn.addEventListener("click", toggleAutoFillStatus);
    if (!input || !box || !wrap) return;
    const show = () => { renderSuggestions(input.value); box.style.display = "block"; };
    input.addEventListener("focus", show);
    input.addEventListener("click", show);
    input.addEventListener("input", show);
    input.addEventListener("blur", () => setTimeout(() => { box.style.display = "none"; }, 150));
    document.addEventListener("click", e => { if (!wrap.contains(e.target)) box.style.display = "none"; });
}

function getSuggestionSource() {
    if (ROLE !== "employee") {
        return SHEETS.map(s => ({
            name: s.name || "",
            url: s.url || "",
            team: s.team || "",
            month_label: "",
        })).filter(s => s.url);
    }

    // Employee: suggest previously used sheets from monthly history.
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
            document.getElementById("sheetUrl").value = item.getAttribute("data-url") || "";
            box.style.display = "none";
            fetchAndRender(item.getAttribute("data-url"), false);
        });
    });
}

// ─── Auto-fill toggle ─────────────────────────────────
async function loadAutoFillStatus() {
    try {
        const data = await (await fetch("/api/auto-fill-status")).json();
        if (data.success) { autoFillEnabled = !!data.enabled; renderAutoToggle(); }
    } catch (_) {}
}

async function toggleAutoFillStatus() {
    try {
        const data = await (await fetch("/api/auto-fill-status", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled: !autoFillEnabled }) })).json();
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
async function saveSheetUrl(sheetUrl) {
    try {
        const data = await (await fetch("/api/save-sheet", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sheet_url: sheetUrl }) })).json();
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
