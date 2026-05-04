(function () {
    if (window.__aiChatWidgetMounted) return;
    window.__aiChatWidgetMounted = true;

    var endpoint = "/api/ai/chat";
    var storageKey = "ads_ai_chat_history_v1";
    var positionKey = "ads_ai_chat_fab_pos_v1";

    function loadHistory() {
        try {
            var raw = localStorage.getItem(storageKey);
            var parsed = raw ? JSON.parse(raw) : [];
            return Array.isArray(parsed) ? parsed : [];
        } catch (_) {
            return [];
        }
    }

    function saveHistory(items) {
        try {
            localStorage.setItem(storageKey, JSON.stringify(items.slice(-20)));
        } catch (_) {}
    }

    function loadPosition() {
        try {
            var raw = localStorage.getItem(positionKey);
            var parsed = raw ? JSON.parse(raw) : null;
            if (!parsed || typeof parsed.x !== "number" || typeof parsed.y !== "number") return null;
            return { x: parsed.x, y: parsed.y };
        } catch (_) {
            return null;
        }
    }

    function savePosition(pos) {
        try {
            localStorage.setItem(positionKey, JSON.stringify(pos));
        } catch (_) {}
    }

    var panel = document.createElement("div");
    panel.className = "ai-chat-panel";
    panel.innerHTML = ""
        + '<div class="ai-chat-header">'
        + '  <div class="ai-chat-title"><i class="fas fa-robot"></i> Chat với AI trợ lý</div>'
        + '  <button type="button" class="ai-chat-close" aria-label="Đóng">×</button>'
        + '</div>'
        + '<div class="ai-chat-body" id="aiChatBody"></div>'
        + '<div class="ai-chat-status" id="aiChatStatus">Sẵn sàng</div>'
        + '<div class="ai-chat-input-row">'
        + '  <input class="ai-chat-input" id="aiChatInput" type="text" placeholder="Hỏi AI về số liệu, ads, vận hành..." maxlength="3000" />'
        + '  <button class="ai-chat-send" id="aiChatSend" type="button">Gửi</button>'
        + '</div>';

    var fab = document.createElement("button");
    fab.type = "button";
    fab.className = "ai-chat-fab";
    fab.setAttribute("aria-label", "Mở chat AI");
    fab.innerHTML = '<i class="fas fa-comment-dots"></i>';

    document.body.appendChild(panel);
    document.body.appendChild(fab);

    var body = panel.querySelector("#aiChatBody");
    var status = panel.querySelector("#aiChatStatus");
    var input = panel.querySelector("#aiChatInput");
    var sendBtn = panel.querySelector("#aiChatSend");
    var closeBtn = panel.querySelector(".ai-chat-close");

    var history = loadHistory();
    var sending = false;
    var drag = { active: false, moved: false, startX: 0, startY: 0, offsetX: 0, offsetY: 0 };
    var fabPos = loadPosition();

    function viewportSize() {
        return {
            w: Math.max(document.documentElement.clientWidth || 0, window.innerWidth || 0),
            h: Math.max(document.documentElement.clientHeight || 0, window.innerHeight || 0),
        };
    }

    function clampFabPosition(x, y) {
        var vp = viewportSize();
        var fw = fab.offsetWidth || 56;
        var fh = fab.offsetHeight || 56;
        var margin = 10;
        var minX = margin;
        var maxX = Math.max(minX, vp.w - fw - margin);
        var minY = 74;
        var maxY = Math.max(minY, vp.h - fh - margin);

        var nx = Math.min(maxX, Math.max(minX, x));
        var ny = Math.min(maxY, Math.max(minY, y));

        // Avoid sensitive top-right area (header actions / critical controls)
        var inTopRightSensitive = nx > vp.w - 300 && ny < 160;
        if (inTopRightSensitive) {
            ny = 160;
        }

        return { x: nx, y: ny };
    }

    function placeFab(x, y, persist) {
        var pos = clampFabPosition(x, y);
        fab.style.left = pos.x + "px";
        fab.style.top = pos.y + "px";
        fab.style.right = "auto";
        fab.style.bottom = "auto";
        fabPos = pos;
        if (persist) savePosition(pos);
        syncPanelPosition();
    }

    function syncPanelPosition() {
        if (!fabPos) return;
        var vp = viewportSize();
        var panelW = Math.min(380, vp.w - 22);
        var panelH = Math.min(560, vp.h - 110);
        var gap = 10;
        var margin = 8;

        var left = fabPos.x + (fab.offsetWidth || 56) - panelW;
        left = Math.min(vp.w - panelW - margin, Math.max(margin, left));

        var preferredTop = fabPos.y - panelH - gap;
        var top = preferredTop;
        if (top < 70) {
            top = fabPos.y + (fab.offsetHeight || 56) + gap;
        }
        top = Math.min(vp.h - panelH - margin, Math.max(70, top));

        panel.style.left = left + "px";
        panel.style.top = top + "px";
        panel.style.right = "auto";
        panel.style.bottom = "auto";
    }

    function setStatus(text) {
        status.textContent = text || "";
    }

    function addMessage(role, text) {
        var div = document.createElement("div");
        div.className = "ai-chat-msg " + (role === "user" ? "user" : "bot");
        div.textContent = text || "";
        body.appendChild(div);
        body.scrollTop = body.scrollHeight;
    }

    function renderHistory() {
        body.innerHTML = "";
        if (!history.length) {
            addMessage("bot", "Dạ đại ca, em là AI trợ lý. Đại ca cứ hỏi trực tiếp, em trả lời ngay.");
            return;
        }
        history.forEach(function (item) {
            addMessage(item.role === "user" ? "user" : "bot", item.content || "");
        });
    }

    function togglePanel(show) {
        var open = typeof show === "boolean" ? show : !panel.classList.contains("open");
        if (open) {
            syncPanelPosition();
            panel.classList.add("open");
            setTimeout(function () { input.focus(); }, 20);
        } else {
            panel.classList.remove("open");
        }
    }

    async function sendMessage() {
        if (sending) return;
        var text = (input.value || "").trim();
        if (!text) return;

        input.value = "";
        history.push({ role: "user", content: text });
        addMessage("user", text);
        saveHistory(history);

        sending = true;
        sendBtn.disabled = true;
        setStatus("AI đang trả lời...");

        try {
            var res = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    message: text,
                    history: history.slice(-10),
                }),
            });
            var rawBody = await res.text();
            var contentType = (res.headers.get("content-type") || "").toLowerCase();
            var data = null;

            if (contentType.indexOf("application/json") >= 0) {
                try {
                    data = rawBody ? JSON.parse(rawBody) : {};
                } catch (_) {
                    data = null;
                }
            }

            if (!data) {
                var preview = (rawBody || "").trim().replace(/\s+/g, " ").slice(0, 160);
                if (preview.toLowerCase().indexOf("<!doctype") === 0 || preview.toLowerCase().indexOf("<html") === 0) {
                    throw new Error("Server chat trả về trang lỗi HTML thay vì JSON. Cần kiểm tra backend/log deploy.");
                }
                throw new Error(preview || "Chat AI trả về dữ liệu không hợp lệ.");
            }

            if (!res.ok || !data.success) {
                throw new Error((data && data.error) || "AI đang bận, thử lại sau.");
            }

            var reply = String(data.reply || "").trim() || "AI không có phản hồi.";
            history.push({ role: "assistant", content: reply });
            addMessage("bot", reply);
            saveHistory(history);
            setStatus("Đã trả lời");
        } catch (err) {
            addMessage("bot", "Em đang lỗi kết nối AI: " + (err.message || "Không rõ lỗi"));
            setStatus("Lỗi kết nối AI");
        } finally {
            sending = false;
            sendBtn.disabled = false;
        }
    }

    fab.addEventListener("pointerdown", function (e) {
        drag.active = true;
        drag.moved = false;
        drag.startX = e.clientX;
        drag.startY = e.clientY;
        var rect = fab.getBoundingClientRect();
        drag.offsetX = e.clientX - rect.left;
        drag.offsetY = e.clientY - rect.top;
        fab.classList.add("dragging");
        try { fab.setPointerCapture(e.pointerId); } catch (_) {}
    });

    fab.addEventListener("pointermove", function (e) {
        if (!drag.active) return;
        var dx = Math.abs(e.clientX - drag.startX);
        var dy = Math.abs(e.clientY - drag.startY);
        if (dx > 4 || dy > 4) drag.moved = true;
        var x = e.clientX - drag.offsetX;
        var y = e.clientY - drag.offsetY;
        placeFab(x, y, false);
    });

    function finishDrag(e) {
        if (!drag.active) return;
        drag.active = false;
        fab.classList.remove("dragging");
        if (drag.moved && fabPos) {
            savePosition(fabPos);
        }
        if (!drag.moved) {
            togglePanel();
        }
        try { fab.releasePointerCapture(e.pointerId); } catch (_) {}
    }

    fab.addEventListener("pointerup", finishDrag);
    fab.addEventListener("pointercancel", finishDrag);

    closeBtn.addEventListener("click", function () { togglePanel(false); });
    sendBtn.addEventListener("click", sendMessage);
    input.addEventListener("keydown", function (e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    window.addEventListener("resize", function () {
        if (!fabPos) return;
        placeFab(fabPos.x, fabPos.y, true);
    });

    (function initPosition() {
        var vp = viewportSize();
        var defaultX = vp.w - 74;
        var defaultY = vp.h - 74;
        var initial = fabPos || { x: defaultX, y: defaultY };
        placeFab(initial.x, initial.y, false);
    })();

    renderHistory();
})();
