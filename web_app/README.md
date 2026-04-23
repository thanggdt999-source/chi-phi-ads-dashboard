# Chi Phí Ads Dashboard - Web App

Trang web hiện đại để quản lý và visualize chi phí quảng cáo từ Google Sheets.

## Tính Năng

✨ **Giao Diện Hiện Đại**
- Thiết kế đẹp, responsive với gradient và animations
- Dễ sử dụng trên desktop và mobile

📊 **Visualization**
- Thống kê tổng chi phí, kết quả, chi phí/kết quả
- Biểu đồ chi phí theo ngày (line chart)
- Biểu đồ chi phí theo sản phẩm (pie chart)
- Bảng dữ liệu chi tiết

🔗 **Tích Hợp Google Sheets**
- Chỉ cần dán link Google Sheet
- Tự động tải dữ liệu từ tab "Chi phí ADS"
- Không cần token bổ sung (dùng service account)

## Installation

1. **Cài đặt dependencies:**
```bash
cd web_app
pip install -r requirements.txt
```

2. **Chạy ứng dụng:**
```bash
python app.py
```

3. **Mở trình duyệt:**
Truy cập http://127.0.0.1:5000

## Sử Dụng

1. Dán link Google Sheet vào ô input
2. Click "Tải Dữ Liệu"
3. Xem thống kê, biểu đồ và bảng dữ liệu ngay lập tức

## Cấu Trúc

```
web_app/
├── app.py                 # Backend Flask
├── requirements.txt       # Dependencies
├── templates/
│   └── index.html        # Frontend HTML
└── static/
    ├── style.css         # Styling
    └── script.js         # JavaScript logic
```

## Lưu Ý

- Cần file `storage/credentials/service_account.json` ở thư mục cha để xác thực Google Sheets
- Sheet phải có tab tên "Chi phí ADS" với dữ liệu có header: Ngày, Số Data, Số tiền chi tiêu - VND, v.v.

## License

All rights reserved © 2026
