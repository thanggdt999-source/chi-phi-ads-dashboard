(function () {
    if (window.__aiChatWidgetMounted) return;
    window.__aiChatWidgetMounted = true;

    var endpoint = "/api/ai/chat";
    var storageKey = "ads_ai_chat_history_v1";

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
            var data = await res.json();
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

    fab.addEventListener("click", function () { togglePanel(); });
    closeBtn.addEventListener("click", function () { togglePanel(false); });
    sendBtn.addEventListener("click", sendMessage);
    input.addEventListener("keydown", function (e) {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    renderHistory();
})();
