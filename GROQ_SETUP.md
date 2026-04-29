# Groq AI Setup Guide

## Groq API Key:
Tạo key từ https://console.groq.com/keys (miễn phí)

## Cách set trên Render Dashboard:

1. Vào: https://dashboard.render.com/web/srv-d7kq877aqgkc73cak9gg/environment
2. Tìm env var `GROQ_API_KEY` (hoặc Create New nếu không có)
3. Paste giá trị key từ Groq console
4. Click Save/Update
5. Service sẽ restart và sử dụng key mới

## Features:
- AI Chat sử dụng Groq API (miễn phí, không cần billing)
- Model: llama-3.3-70b-versatile
- Max tokens: 500
- Hỗ trợ tiếng Việt
- Có context từ Google Sheets data

## Test:
Vào dashboard → AI Chat → gửi tin nhắn → AI sẽ trả lời dựa trên dữ liệu sheet
