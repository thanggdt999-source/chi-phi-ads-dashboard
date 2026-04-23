from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from google.oauth2.service_account import Credentials
import gspread
import re
import os
import csv
from pathlib import Path
from functools import wraps
from datetime import datetime
import json

app = Flask(__name__)
app.secret_key = os.getenv("WEB_APP_SECRET_KEY", "change-this-secret-in-production")

APP_USERNAME = os.getenv("WEB_APP_USERNAME", "admin")
APP_PASSWORD = os.getenv("WEB_APP_PASSWORD", "admin123")

SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "storage" / "credentials" / "service_account.json"
SHEET_URLS_PATH = Path(os.getenv("SHEET_URLS_PATH", str(Path(__file__).parent.parent / "storage" / "sheet_urls.csv")))
AUTO_STATE_PATH = Path(os.getenv("AUTO_STATE_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "auto_fill_state.json")))


def is_logged_in():
    return session.get("logged_in") is True


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for("login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapper


def api_login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return jsonify({"success": False, "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."}), 401
        return view_func(*args, **kwargs)

    return wrapper


def load_auto_fill_enabled() -> bool:
    AUTO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not AUTO_STATE_PATH.exists():
        payload = {
            "enabled": True,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        with AUTO_STATE_PATH.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return True

    try:
        with AUTO_STATE_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return bool(payload.get("enabled", True))
    except Exception:
        return True


def save_auto_fill_enabled(enabled: bool) -> None:
    AUTO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "enabled": bool(enabled),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with AUTO_STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def get_gspread_client():
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Uu tien credentials tu bien moi truong de deploy cloud (Render, Railway, ...)
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if service_account_json:
        service_account_info = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
        return gspread.authorize(creds)

    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(
            "Service account not found. Set GOOGLE_SERVICE_ACCOUNT_JSON or provide local file: "
            f"{SERVICE_ACCOUNT_PATH}"
        )

    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_PATH), scopes=scopes)
    return gspread.authorize(creds)

def extract_sheet_id(url):
    """Extract sheet ID from Google Sheets URL."""
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', url)
    return match.group(1) if match else None

DISPLAY_COLUMNS = ["Ngày", "Tên tài khoản", "Tên sản phẩm - VN", "Số Data", "Số tiền chi tiêu - VND"]

def fetch_chi_phi_ads_data(sheet_id):
    """Fetch all data from 'Chi phí ADS' tab."""
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet("Chi phí ADS")
        
        all_values = worksheet.get_all_values()
        
        if not all_values:
            return {"success": True, "data": [], "headers": DISPLAY_COLUMNS}
        
        # Header ở row 1 (index 0)
        raw_headers = [h.strip() for h in all_values[0]]
        
        # Map header -> index
        col_idx = {}
        for col in DISPLAY_COLUMNS:
            for i, h in enumerate(raw_headers):
                if h == col:
                    col_idx[col] = i
                    break
        
        # Data từ row 3 trở đi (index 2), bỏ row 2 là row tổng
        rows = []
        for i in range(2, len(all_values)):
            row = all_values[i]
            if not row or not row[0].strip():
                continue
            
            row_data = {}
            for col in DISPLAY_COLUMNS:
                idx = col_idx.get(col)
                value = row[idx].strip() if idx is not None and idx < len(row) else ""
                # Bỏ qua dòng có lỗi công thức
                if "#N/A" in value or "#REF" in value or "#VALUE" in value:
                    value = ""
                row_data[col] = value
            
            rows.append(row_data)
        
        # Đọc % Ads từ tab TỔNG, cột L, dòng 3 (index [2][11])
        ads_percent = ""
        try:
            tong_ws = spreadsheet.worksheet("TỔNG")
            tong_vals = tong_ws.get_all_values()
            if len(tong_vals) >= 3 and len(tong_vals[2]) >= 12:
                ads_percent = tong_vals[2][11].strip()  # cột L = index 11
        except Exception:
            pass

        return {"success": True, "data": rows, "headers": DISPLAY_COLUMNS, "ads_percent": ads_percent}
    except Exception as e:
        raw_error = str(e)
        lower_error = raw_error.lower()

        if "<response [404]>" in lower_error:
            return {
                "success": False,
                "error": "Không tìm thấy sheet hoặc service account chưa được cấp quyền truy cập."
            }

        if "permission" in lower_error or "forbidden" in lower_error or "<response [403]>" in lower_error:
            return {
                "success": False,
                "error": "Sheet chưa chia sẻ cho service account (Editor)."
            }

        if "worksheetnotfound" in lower_error:
            return {
                "success": False,
                "error": "Không tìm thấy tab 'Chi phí ADS' trong sheet này."
            }

        return {"success": False, "error": raw_error}

@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if is_logged_in():
            return redirect(url_for("index"))
        return render_template("login.html", error="")

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")

    if username == APP_USERNAME and password == APP_PASSWORD:
        session["logged_in"] = True
        next_url = request.args.get("next") or url_for("index")
        return redirect(next_url)

    return render_template("login.html", error="Sai tài khoản hoặc mật khẩu")


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/api/fetch-data", methods=["POST"])
@api_login_required
def fetch_data():
    data = request.get_json()
    sheet_url = data.get("sheet_url", "").strip()
    
    if not sheet_url:
        return jsonify({"success": False, "error": "Vui lòng nhập URL sheet"}), 400
    
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({"success": False, "error": "URL sheet không hợp lệ"}), 400
    
    result = fetch_chi_phi_ads_data(sheet_id)
    return jsonify(result)


@app.route("/api/auto-fill-status", methods=["GET"])
@api_login_required
def auto_fill_status():
    return jsonify({"success": True, "enabled": load_auto_fill_enabled()})


@app.route("/api/auto-fill-status", methods=["POST"])
@api_login_required
def update_auto_fill_status():
    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled", False))
    save_auto_fill_enabled(enabled)
    return jsonify({"success": True, "enabled": enabled})

@app.route("/api/list-sheets", methods=["GET"])
@api_login_required
def list_sheets():
    """Trả về danh sách sheet đã lưu dạng [{name, url}]."""
    sheets = []
    if SHEET_URLS_PATH.exists():
        with open(SHEET_URLS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if "," in line:
                    # Format mới: name,url
                    name, url = line.split(",", 1)
                    sheets.append({"name": name.strip(), "url": url.strip()})
                else:
                    # Format cũ: chỉ url → dùng sheet_id làm name tạm
                    sheet_id = extract_sheet_id(line)
                    sheets.append({"name": sheet_id or line, "url": line})
    return jsonify({"success": True, "sheets": sheets})

@app.route("/api/save-sheet", methods=["POST"])
@api_login_required
def save_sheet():
    data = request.get_json()
    sheet_url = data.get("sheet_url", "").strip()
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({"success": False, "error": "URL không hợp lệ"}), 400

    # Đọc file hiện tại
    existing_lines = []
    if SHEET_URLS_PATH.exists():
        with open(SHEET_URLS_PATH, "r", encoding="utf-8") as f:
            existing_lines = [l.strip() for l in f if l.strip()]

    if any(sheet_id in l for l in existing_lines):
        return jsonify({"success": True, "message": "Sheet đã có trong danh sách tự động điền", "already_exists": True})

    # Lấy tên sheet từ Google Sheets title
    sheet_name = ""
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        sheet_name = spreadsheet.title
    except Exception:
        sheet_name = sheet_id

    clean_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    with open(SHEET_URLS_PATH, "a", encoding="utf-8") as f:
        f.write(f"{sheet_name},{clean_url}\n")

    return jsonify({"success": True, "message": f'Đã lưu sheet "{sheet_name}" vào danh sách!', "name": sheet_name})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
