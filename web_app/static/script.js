let currentData = {
    rows: [],
    headers: [],
    ads_percent: ""
};

let autoFillEnabled = true;

let savedSheets = [];

let charts = {
    spendByDate: null,
    spendByProduct: null
};

// Load saved sheet list for suggestions
async function loadSavedSheets() {
    try {
        const res = await fetch("/api/list-sheets");
        const data = await res.json();
        savedSheets = data.success && Array.isArray(data.sheets) ? data.sheets : [];
    } catch (e) {
        savedSheets = [];
    }
}

function renderSuggestions(filterText = "") {
    const box = document.getElementById("sheetSuggestions");
    if (!box) return;

    const q = filterText.trim().toLowerCase();
    const list = savedSheets.filter((s) => {
        const name = (s.name || "").toLowerCase();
        const url = (s.url || "").toLowerCase();
        return !q || name.includes(q) || url.includes(q);
    });

    if (list.length === 0) {
        box.innerHTML = '<div class="suggestion-empty">Không có gợi ý phù hợp</div>';
    } else {
        box.innerHTML = list.map((s) => `
            <button type="button" class="suggestion-item" data-url="${s.url.replace(/"/g, "&quot;")}">
                <span class="suggestion-name">${s.name}</span>
                <span class="suggestion-url">${s.url}</span>
            </button>
        `).join("");
    }

    box.querySelectorAll(".suggestion-item").forEach((item) => {
        item.addEventListener("mousedown", () => {
            const url = item.getAttribute("data-url") || "";
            document.getElementById("sheetUrl").value = url;
            box.style.display = "none";
            fetchAndRender(url, false);
        });
    });
}

async function fetchAndRender(sheetUrl, shouldAutoSave = true) {
    const loadingSpinner = document.getElementById("loadingSpinner");
    const errorDiv = document.getElementById("errorMessage");
    errorDiv.style.display = "none";
    loadingSpinner.style.display = "block";

    try {
        const response = await fetch("/api/fetch-data", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sheet_url: sheetUrl })
        });
        const data = await response.json();
        loadingSpinner.style.display = "none";

        if (!data.success) { showError("❌ " + (data.error || "Không thể tải dữ liệu")); return; }
        if (!data.data || data.data.length === 0) { showError("⚠️ Sheet không có dữ liệu trong tab 'Chi phí ADS'"); return; }

        currentData = { rows: data.data, headers: data.headers, ads_percent: data.ads_percent || "" };
        renderData();

        if (shouldAutoSave) {
            saveSheetUrl(sheetUrl);
        }
    } catch (e) {
        loadingSpinner.style.display = "none";
        showError("❌ Lỗi kết nối: " + e.message);
    }
}

async function handleSubmit(event) {
    event.preventDefault();

    const sheetUrl = document.getElementById("sheetUrl").value.trim();
    fetchAndRender(sheetUrl, true);
}

function renderAutoToggle() {
    const btn = document.getElementById("autoFillToggleBtn");
    if (!btn) return;

    btn.classList.remove("auto-on", "auto-off");
    if (autoFillEnabled) {
        btn.classList.add("auto-on");
        btn.textContent = "Auto Fill: ON";
    } else {
        btn.classList.add("auto-off");
        btn.textContent = "Auto Fill: OFF";
    }
}

async function loadAutoFillStatus() {
    try {
        const res = await fetch("/api/auto-fill-status");
        const data = await res.json();
        if (data.success) {
            autoFillEnabled = !!data.enabled;
            renderAutoToggle();
        }
    } catch (e) {
        // Keep default state in UI if API fails.
    }
}

async function toggleAutoFillStatus() {
    const nextState = !autoFillEnabled;
    try {
        const res = await fetch("/api/auto-fill-status", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ enabled: nextState })
        });
        const data = await res.json();
        if (data.success) {
            autoFillEnabled = !!data.enabled;
            renderAutoToggle();
            showToast(autoFillEnabled ? "✅ Đã bật tự động điền chi phí" : "⏸️ Đã tắt tự động điền chi phí");
        } else {
            showError("❌ Không đổi được trạng thái Auto Fill");
        }
    } catch (e) {
        showError("❌ Lỗi khi đổi trạng thái Auto Fill");
    }
}

document.addEventListener("DOMContentLoaded", async () => {
    await loadSavedSheets();
    await loadAutoFillStatus();

    const input = document.getElementById("sheetUrl");
    const suggestionBox = document.getElementById("sheetSuggestions");
    const inputWrap = document.querySelector(".input-wrap");
    const autoToggleBtn = document.getElementById("autoFillToggleBtn");

    if (!input || !suggestionBox || !inputWrap) return;

    if (autoToggleBtn) {
        autoToggleBtn.addEventListener("click", toggleAutoFillStatus);
    }

    input.addEventListener("focus", () => {
        renderSuggestions(input.value);
        suggestionBox.style.display = "block";
    });

    input.addEventListener("click", () => {
        renderSuggestions(input.value);
        suggestionBox.style.display = "block";
    });

    input.addEventListener("input", () => {
        renderSuggestions(input.value);
        suggestionBox.style.display = "block";
    });

    input.addEventListener("blur", () => {
        setTimeout(() => {
            suggestionBox.style.display = "none";
        }, 150);
    });

    document.addEventListener("click", (e) => {
        if (!inputWrap.contains(e.target)) {
            suggestionBox.style.display = "none";
        }
    });
});

function renderData() {
    if (!currentData.rows || currentData.rows.length === 0) {
        showError("Không có dữ liệu trong sheet");
        return;
    }

    renderStats();
    renderTable();
    renderCharts();

    document.getElementById("statsSection").style.display = "block";
    document.getElementById("tableSection").style.display = "block";
    document.getElementById("chartsSection").style.display = "grid";
}

function renderStats() {
    const rows = currentData.rows;

    let totalSpend = 0;
    let totalResults = 0;

    rows.forEach(row => {
        const spendRaw = (row["Số tiền chi tiêu - VND"] || "").replace(/\./g, "").replace(/,/g, ".");
        const spend = parseFloat(spendRaw.replace(/[^\d.]/g, "")) || 0;
        const results = parseInt((row["Số Data"] || "").replace(/[^\d]/g, "")) || 0;

        totalSpend += spend;
        totalResults += results;
    });

    const costPerResult = totalResults > 0 ? Math.round(totalSpend / totalResults) : 0;

    const adsPercent = currentData.ads_percent || "—";

    document.getElementById("totalSpend").textContent = formatCurrency(totalSpend);
    document.getElementById("totalResults").textContent = totalResults.toLocaleString("en-US");
    document.getElementById("costPerResult").textContent = formatCurrency(costPerResult);
    document.getElementById("adsPercent").textContent = adsPercent;
}

function renderTable() {
    const headers = currentData.headers;
    const rows = currentData.rows;

    // Render header
    const tableHeader = document.getElementById("tableHeader");
    tableHeader.innerHTML = "";
    headers.forEach(header => {
        const th = document.createElement("th");
        th.textContent = header;
        tableHeader.appendChild(th);
    });

    // Render body
    const tableBody = document.getElementById("tableBody");
    tableBody.innerHTML = "";
    rows.forEach(row => {
        const tr = document.createElement("tr");
        headers.forEach(header => {
            const td = document.createElement("td");
            td.textContent = row[header] || "-";
            tr.appendChild(td);
        });
        tableBody.appendChild(tr);
    });
}

function renderCharts() {
    renderSpendByDateChart();
    renderSpendByProductChart();
}

function renderSpendByDateChart() {
    const rows = currentData.rows;
    const spendByDate = {};

    rows.forEach(row => {
        const date = row["Ngày"] || "Unknown";
        const spendRaw2 = (row["Số tiền chi tiêu - VND"] || "").replace(/\./g, "").replace(/,/g, ".");
        const spend = parseFloat(spendRaw2.replace(/[^\d.]/g, "")) || 0;
        spendByDate[date] = (spendByDate[date] || 0) + spend;
    });

    const labels = Object.keys(spendByDate).sort();
    const data = labels.map(date => spendByDate[date]);

    const ctx = document.getElementById("spendByDateChart").getContext("2d");
    
    if (charts.spendByDate) {
        charts.spendByDate.destroy();
    }

    charts.spendByDate = new Chart(ctx, {
        type: "line",
        data: {
            labels: labels,
            datasets: [{
                label: "Chi Phí (VND)",
                data: data,
                borderColor: "#3498db",
                backgroundColor: "rgba(52, 152, 219, 0.1)",
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 5,
                pointBackgroundColor: "#3498db",
                pointBorderColor: "#fff",
                pointBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: "top"
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return formatShortCurrency(value);
                        }
                    }
                }
            }
        }
    });
}

function renderSpendByProductChart() {
    const rows = currentData.rows;
    const spendByProduct = {};

    rows.forEach(row => {
        const product = row["Tên sản phẩm - VN"] || "Unknown";
        const spendRaw3 = (row["Số tiền chi tiêu - VND"] || "").replace(/\./g, "").replace(/,/g, ".");
        const spend = parseFloat(spendRaw3.replace(/[^\d.]/g, "")) || 0;
        spendByProduct[product] = (spendByProduct[product] || 0) + spend;
    });

    const labels = Object.keys(spendByProduct);
    const data = labels.map(product => spendByProduct[product]);

    const colors = [
        "#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6",
        "#1abc9c", "#34495e", "#e67e22", "#c0392b", "#27ae60"
    ];

    const ctx = document.getElementById("spendByProductChart").getContext("2d");
    
    if (charts.spendByProduct) {
        charts.spendByProduct.destroy();
    }

    charts.spendByProduct = new Chart(ctx, {
        type: "doughnut",
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors.slice(0, labels.length),
                borderColor: "#fff",
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: true,
                    position: "bottom"
                }
            }
        }
    });
}

function showError(message) {
    const errorDiv = document.getElementById("errorMessage");
    errorDiv.textContent = message;
    errorDiv.style.display = "block";
}

async function saveSheetUrl(sheetUrl) {
    try {
        const response = await fetch("/api/save-sheet", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ sheet_url: sheetUrl })
        });
        const data = await response.json();
        if (data.success && !data.already_exists) {
            showToast("✅ " + data.message);
            await loadSavedSheets();
        }
    } catch (e) {
        // Silent fail - không ảnh hưởng đến việc xem dữ liệu
    }
}

function showToast(message) {
    let toast = document.getElementById("toastNotification");
    if (!toast) {
        toast = document.createElement("div");
        toast.id = "toastNotification";
        toast.style.cssText = `
            position: fixed; bottom: 24px; right: 24px; z-index: 9999;
            background: #22c55e; color: white; padding: 12px 20px;
            border-radius: 8px; font-size: 14px; font-weight: 500;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            transition: opacity 0.4s ease;
        `;
        document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.style.opacity = "1";
    clearTimeout(toast._timeout);
    toast._timeout = setTimeout(() => { toast.style.opacity = "0"; }, 3500);
}

function showSuccess() {
    const errorDiv = document.getElementById("errorMessage");
    errorDiv.style.display = "none";
}

function formatCurrency(value) {
    return value.toLocaleString("vi-VN", {
        style: "currency",
        currency: "VND"
    }).replace("₫", "").trim() + " VND";
}

function formatShortCurrency(value) {
    if (value >= 1000000) {
        return (value / 1000000).toFixed(1) + "M";
    } else if (value >= 1000) {
        return (value / 1000).toFixed(0) + "K";
    }
    return value.toFixed(0);
}

// page events are initialized in the DOMContentLoaded block above
