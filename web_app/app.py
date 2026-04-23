from flask import Flask, render_template, request, jsonify, session, redirect, url_for
from google.oauth2.service_account import Credentials
import gspread
import re
import os
from pathlib import Path
from functools import wraps
from datetime import datetime
import json
from typing import Optional

app = Flask(__name__)
app.secret_key = os.getenv("WEB_APP_SECRET_KEY", "change-this-secret-in-production")

SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "storage" / "credentials" / "service_account.json"
SHEET_URLS_PATH = Path(os.getenv("SHEET_URLS_PATH", str(Path(__file__).parent.parent / "storage" / "sheet_urls.csv")))
AUTO_STATE_PATH = Path(os.getenv("AUTO_STATE_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "auto_fill_state.json")))
USERS_FILE_PATH = Path(os.getenv("USERS_FILE_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "users.json")))

ROLE_LEVELS = {"admin": 3, "lead": 2, "employee": 1}
TEAM_CODES = ["TEAM_1", "TEAM_2", "TEAM_3", "TEAM_4", "TEAM_5"]
SHARED_LOGIN_USERNAME = os.getenv("SHARED_LOGIN_USERNAME", "fbads.gdt")
LOGIN_BOARD_NAME = os.getenv("LOGIN_BOARD_NAME", "Bang Chi Phi Ads")

# ─────────────────── USER CONFIG ───────────────────

def extract_display_name(sheet_name: str) -> str:
    if "-" in sheet_name:
        parts = [p.strip() for p in sheet_name.split("-") if p.strip()]
        if parts:
            return parts[-1]
    return sheet_name.strip() or "Nhân viên"


def slugify_username(text: str) -> str:
    lowered = text.lower()
    replaced = re.sub(r"[^a-z0-9]+", "_", lowered)
    compact = re.sub(r"_+", "_", replaced).strip("_")
    return compact or "user"


def infer_team_from_sheet_name(sheet_name: str, index: int) -> str:
    lower = sheet_name.lower()
    for i in range(1, 6):
        if f"team {i}" in lower or f"team{i}" in lower or f"_t{i}" in lower or f"-t{i}" in lower:
            return f"TEAM_{i}"
    return TEAM_CODES[index % len(TEAM_CODES)]


def parse_sheet_registry() -> list:
    sheets = []
    if not SHEET_URLS_PATH.exists():
        return sheets
    with SHEET_URLS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if "," in line:
                name, url = line.split(",", 1)
                sheets.append({"name": name.strip(), "url": url.strip()})
            else:
                sheets.append({"name": line.strip(), "url": line.strip()})
    return sheets


def build_auto_users_from_sheet_urls() -> dict:
    sheets = parse_sheet_registry()
    users = {
        "admin_root": {
            "password": os.getenv("DEFAULT_ADMIN_PASSWORD", "Admin@Hexi2026!"),
            "role": "admin",
            "display_name": "System Admin",
        },
        "fbads.gdt": {
            "password": os.getenv("DEFAULT_EMPLOYEE_PASSWORD", "123"),
            "role": "employee",
            "display_name": "Nhân viên",
        }
    }

    for i, team_code in enumerate(TEAM_CODES, start=1):
        users[f"lead_team_{i}"] = {
            "password": os.getenv(f"LEAD_TEAM_{i}_PASSWORD", f"LeadTeam{i}@2026"),
            "role": "lead",
            "team": team_code,
            "display_name": f"Lead {team_code}",
        }

    username_count = {}
    for idx, sheet in enumerate(sheets):
        raw_name = sheet.get("name", "")
        display_name = extract_display_name(raw_name)
        team_code = infer_team_from_sheet_name(raw_name, idx)
        base = slugify_username(f"emp_{display_name}")
        username_count[base] = username_count.get(base, 0) + 1
        suffix = username_count[base]
        uname = base if suffix == 1 else f"{base}_{suffix}"
        users[uname] = {
            "password": os.getenv("DEFAULT_EMPLOYEE_PASSWORD", "Emp@123456"),
            "role": "employee",
            "team": team_code,
            "display_name": display_name,
            "sheet_url": sheet.get("url", ""),
        }
    return users


def write_default_users_file(users: dict) -> None:
    USERS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with USERS_FILE_PATH.open("w", encoding="utf-8") as f:
        json.dump(users, f, ensure_ascii=False, indent=2)


def load_users_config() -> dict:
    """Load user config from env, file, or auto-generated defaults."""
    config_json = os.getenv("USERS_CONFIG", "").strip()
    if config_json:
        try:
            return json.loads(config_json)
        except Exception:
            pass

    if USERS_FILE_PATH.exists():
        try:
            with USERS_FILE_PATH.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict) and payload:
                return payload
        except Exception:
            pass

    auto_users = build_auto_users_from_sheet_urls()
    if auto_users:
        try:
            write_default_users_file(auto_users)
        except Exception:
            pass
        return auto_users

    # Legacy fallback
    username = os.getenv("WEB_APP_USERNAME", "admin")
    password = os.getenv("WEB_APP_PASSWORD", "admin123")
    return {
        username: {"password": password, "role": "admin", "display_name": "Administrator"}
    }


def get_user(username: str) -> Optional[dict]:
    return load_users_config().get(username)


def get_shared_login_user() -> Optional[dict]:
    return get_user(SHARED_LOGIN_USERNAME)


def set_session_user(username: str, user: dict, *, elevated: bool = False) -> None:
    session["logged_in"] = True
    session["username"] = username
    session["role"] = user.get("role", "employee")
    session["team"] = user.get("team", "")
    session["display_name"] = user.get("display_name", username)
    session["sheet_url"] = user.get("sheet_url", "")
    session["is_elevated"] = elevated


def get_accessible_sheets_for_user(username: str) -> list:
    """Return [{name, url, team, username}] accessible to this user."""
    users = load_users_config()
    user = users.get(username, {})
    role = user.get("role", "employee")
    team = user.get("team", "")

    if role == "admin":
        return [
            {
                "name": udata.get("display_name", uname),
                "url": udata["sheet_url"],
                "team": udata.get("team", ""),
                "username": uname,
            }
            for uname, udata in users.items()
            if udata.get("role") == "employee" and udata.get("sheet_url")
        ]

    if role == "lead":
        return [
            {
                "name": udata.get("display_name", uname),
                "url": udata["sheet_url"],
                "team": team,
                "username": uname,
            }
            for uname, udata in users.items()
            if udata.get("role") == "employee" and udata.get("team") == team and udata.get("sheet_url")
        ]

    # employee
    sheet_url = user.get("sheet_url", "")
    if sheet_url:
        return [{"name": user.get("display_name", username), "url": sheet_url, "team": team, "username": username}]

    # Shared employee account: can only open individual employee sheets, not aggregated views.
    if role == "employee":
        return [
            {
                "name": udata.get("display_name", uname),
                "url": udata["sheet_url"],
                "team": udata.get("team", ""),
                "username": uname,
            }
            for uname, udata in users.items()
            if udata.get("role") == "employee" and udata.get("sheet_url")
        ]
    return []


# ─────────────────── AUTH HELPERS ───────────────────

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


def api_role_required(min_role: str):
    """Decorator: require minimum role level for an API endpoint."""
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            if not is_logged_in():
                return jsonify({"success": False, "error": "Chưa đăng nhập."}), 401
            user_level = ROLE_LEVELS.get(session.get("role", "employee"), 0)
            required_level = ROLE_LEVELS.get(min_role, 99)
            if user_level < required_level:
                return jsonify({"success": False, "error": "Bạn không có quyền truy cập tính năng này."}), 403
            return view_func(*args, **kwargs)
        return wrapper
    return decorator


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


def parse_spend(val: str) -> float:
    """Parse Vietnamese-format currency string to float."""
    cleaned = val.replace(".", "").replace(",", ".")
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_int(val: str) -> int:
    cleaned = re.sub(r"[^\d]", "", val)
    return int(cleaned) if cleaned else 0

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
    username = session.get("username", "")
    role = session.get("role", "employee")
    display_name = session.get("display_name", username)
    team = session.get("team", "")
    sheet_url = session.get("sheet_url", "")
    accessible_sheets = get_accessible_sheets_for_user(username)
    return render_template(
        "index.html",
        role=role,
        display_name=display_name,
        team=team,
        sheet_url=sheet_url,
        accessible_sheets_count=len(accessible_sheets),
        accessible_sheets_json=json.dumps(accessible_sheets, ensure_ascii=False),
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    shared_user = get_shared_login_user()
    if request.method == "GET":
        if is_logged_in():
            return redirect(url_for("index"))
        return render_template(
            "login.html",
            error="",
            title="Đăng nhập hệ thống",
            board_name=LOGIN_BOARD_NAME,
            form_action=url_for("login"),
            submit_label="Vào hệ thống",
            show_back_link=False,
        )

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")

    if shared_user and username == SHARED_LOGIN_USERNAME and shared_user.get("password") == password:
        set_session_user(SHARED_LOGIN_USERNAME, shared_user, elevated=False)
        next_url = request.args.get("next") or url_for("index")
        return redirect(next_url)

    return render_template(
        "login.html",
        error="Sai tài khoản hoặc mật khẩu",
        title="Đăng nhập hệ thống",
        board_name=LOGIN_BOARD_NAME,
        form_action=url_for("login"),
        submit_label="Vào hệ thống",
        show_back_link=False,
    )


@app.route("/privileged-login", methods=["GET", "POST"])
@login_required
def privileged_login():
    if ROLE_LEVELS.get(session.get("role", "employee"), 0) >= ROLE_LEVELS["lead"]:
        return redirect(url_for("index"))

    if request.method == "GET":
        return render_template(
            "login.html",
            error="",
            title="Đăng nhập Leader / Admin",
            board_name=LOGIN_BOARD_NAME,
            form_action=url_for("privileged_login"),
            submit_label="Mở quyền xem tổng",
            show_back_link=True,
        )

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    user = get_user(username)

    if user and user.get("password") == password and user.get("role") in {"lead", "admin"}:
        set_session_user(username, user, elevated=True)
        return redirect(url_for("index"))

    return render_template(
        "login.html",
        error="Chỉ tài khoản leader hoặc admin mới dùng được bước 2",
        title="Đăng nhập Leader / Admin",
        board_name=LOGIN_BOARD_NAME,
        form_action=url_for("privileged_login"),
        submit_label="Mở quyền xem tổng",
        show_back_link=True,
    )


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/api/fetch-data", methods=["POST"])
@api_login_required
def fetch_data():
    data = request.get_json()
    sheet_url = (data.get("sheet_url") or "").strip()
    
    if not sheet_url:
        return jsonify({"success": False, "error": "Vui lòng nhập URL sheet"}), 400
    
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({"success": False, "error": "URL sheet không hợp lệ"}), 400
    
    role = session.get("role", "employee")
    username = session.get("username", "")

    # Access control
    if role == "employee":
        accessible_ids = {extract_sheet_id(s["url"]) for s in get_accessible_sheets_for_user(username) if s.get("url")}
        if sheet_id not in accessible_ids:
            return jsonify({"success": False, "error": "Bạn không có quyền xem sheet này."}), 403
    elif role == "lead":
        accessible_ids = {extract_sheet_id(s["url"]) for s in get_accessible_sheets_for_user(username)}
        if sheet_id not in accessible_ids:
            return jsonify({"success": False, "error": "Sheet này không thuộc team của bạn."}), 403

    result = fetch_chi_phi_ads_data(sheet_id)
    return jsonify(result)


@app.route("/api/fetch-all-data", methods=["POST"])
@api_role_required("lead")
def fetch_all_data():
    """Fetch & aggregate data from all sheets accessible to the current user."""
    username = session.get("username", "")
    sheets = get_accessible_sheets_for_user(username)
    if not sheets:
        return jsonify({"success": False, "error": "Không tìm thấy sheet nào cho tài khoản này."}), 404

    all_rows = []
    member_summaries = []
    errors = []

    for sheet in sheets:
        sheet_id = extract_sheet_id(sheet["url"])
        if not sheet_id:
            continue
        result = fetch_chi_phi_ads_data(sheet_id)
        if result.get("success"):
            member_rows = result.get("data", [])
            all_rows.extend(member_rows)
            total_spend = sum(parse_spend(r.get("Số tiền chi tiêu - VND", "")) for r in member_rows)
            total_data = sum(parse_int(r.get("Số Data", "")) for r in member_rows)
            member_summaries.append({
                "name": sheet["name"],
                "team": sheet.get("team", ""),
                "total_spend": total_spend,
                "total_data": total_data,
                "cost_per_data": round(total_spend / total_data) if total_data > 0 else 0,
                "sheet_url": sheet["url"],
            })
        else:
            errors.append({"name": sheet["name"], "error": result.get("error", "Lỗi không xác định")})

    # Sort rankings by total_spend desc
    member_summaries.sort(key=lambda x: x["total_spend"], reverse=True)
    for i, m in enumerate(member_summaries):
        m["rank"] = i + 1

    return jsonify({
        "success": True,
        "data": all_rows,
        "headers": DISPLAY_COLUMNS,
        "member_summaries": member_summaries,
        "errors": errors,
    })


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
    """Return sheets accessible to current user (role-filtered)."""
    username = session.get("username", "")
    accessible = get_accessible_sheets_for_user(username)
    sheets = [{"name": s["name"], "url": s["url"], "team": s.get("team", "")} for s in accessible]
    return jsonify({"success": True, "sheets": sheets})

# ─────────────────── ADMIN USER MANAGEMENT ───────────────────

def save_users_config(config: dict) -> None:
    """Persist user config to the JSON file (used by admin UI)."""
    USERS_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with USERS_FILE_PATH.open("w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def admin_page_required(view_func):
    """Decorator for admin-only HTML pages (redirect to login/403 page)."""
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for("login", next=request.path))
        if ROLE_LEVELS.get(session.get("role", ""), 0) < ROLE_LEVELS["admin"]:
            return render_template("403.html"), 403
        return view_func(*args, **kwargs)
    return wrapper


@app.route("/admin/users")
@admin_page_required
def admin_users_page():
    users = load_users_config()
    # Mask passwords before sending to template
    users_safe = {
        uname: {k: v for k, v in udata.items() if k != "password"}
        for uname, udata in users.items()
    }
    return render_template(
        "admin_users.html",
        users_json=json.dumps(users_safe, ensure_ascii=False),
        team_codes=TEAM_CODES,
        display_name=session.get("display_name", ""),
    )


@app.route("/api/admin/users", methods=["GET"])
@api_role_required("admin")
def api_admin_list_users():
    users = load_users_config()
    result = []
    for uname, udata in users.items():
        result.append({
            "username": uname,
            "role": udata.get("role", "employee"),
            "team": udata.get("team", ""),
            "display_name": udata.get("display_name", uname),
            "sheet_url": udata.get("sheet_url", ""),
        })
    return jsonify({"success": True, "users": result})


@app.route("/api/admin/users", methods=["POST"])
@api_role_required("admin")
def api_admin_create_user():
    data = request.get_json(silent=True) or {}
    username = data.get("username", "").strip()
    password = data.get("password", "").strip()
    role = data.get("role", "employee").strip()
    team = data.get("team", "").strip()
    display_name = data.get("display_name", "").strip()
    sheet_url = data.get("sheet_url", "").strip()

    if not username or not password:
        return jsonify({"success": False, "error": "Tên đăng nhập và mật khẩu không được để trống."}), 400
    if role not in ROLE_LEVELS:
        return jsonify({"success": False, "error": "Vai trò không hợp lệ."}), 400
    if role in ("lead", "employee") and team not in TEAM_CODES:
        return jsonify({"success": False, "error": "Vui lòng chọn team hợp lệ."}), 400

    users = load_users_config()
    if username in users:
        return jsonify({"success": False, "error": f'Tên đăng nhập "{username}" đã tồn tại.'}), 409

    users[username] = {
        "password": password,
        "role": role,
        "display_name": display_name or username,
    }
    if team:
        users[username]["team"] = team
    if role == "employee" and sheet_url:
        users[username]["sheet_url"] = sheet_url

    save_users_config(users)
    return jsonify({"success": True, "message": f'Đã tạo tài khoản "{username}".'})


@app.route("/api/admin/users/<username>", methods=["PUT"])
@api_role_required("admin")
def api_admin_update_user(username):
    data = request.get_json(silent=True) or {}
    users = load_users_config()

    if username not in users:
        return jsonify({"success": False, "error": "Người dùng không tồn tại."}), 404

    role = data.get("role", users[username].get("role", "employee")).strip()
    if username == SHARED_LOGIN_USERNAME and role != "employee":
        return jsonify({"success": False, "error": "Tài khoản đăng nhập chung phải giữ vai trò employee."}), 400
    team = data.get("team", users[username].get("team", "")).strip()
    display_name = data.get("display_name", users[username].get("display_name", username)).strip()
    sheet_url = data.get("sheet_url", users[username].get("sheet_url", "")).strip()
    new_password = data.get("password", "").strip()

    if role not in ROLE_LEVELS:
        return jsonify({"success": False, "error": "Vai trò không hợp lệ."}), 400
    if role in ("lead", "employee") and team not in TEAM_CODES:
        return jsonify({"success": False, "error": "Vui lòng chọn team hợp lệ."}), 400

    # Prevent removing the last admin
    if users[username].get("role") == "admin" and role != "admin":
        admin_count = sum(1 for u in users.values() if u.get("role") == "admin")
        if admin_count <= 1:
            return jsonify({"success": False, "error": "Không thể hạ quyền admin duy nhất."}), 400

    users[username]["role"] = role
    users[username]["display_name"] = display_name or username
    if team:
        users[username]["team"] = team
    elif "team" in users[username] and role == "admin":
        users[username].pop("team", None)
    if role == "employee":
        users[username]["sheet_url"] = sheet_url
    else:
        users[username].pop("sheet_url", None)
    if new_password:
        users[username]["password"] = new_password

    save_users_config(users)
    return jsonify({"success": True, "message": f'Đã cập nhật tài khoản "{username}".'})


@app.route("/api/admin/users/<username>", methods=["DELETE"])
@api_role_required("admin")
def api_admin_delete_user(username):
    users = load_users_config()

    if username not in users:
        return jsonify({"success": False, "error": "Người dùng không tồn tại."}), 404

    if username == SHARED_LOGIN_USERNAME:
        return jsonify({"success": False, "error": "Không thể xoá tài khoản đăng nhập chung."}), 400

    # Prevent deleting last admin
    if users[username].get("role") == "admin":
        admin_count = sum(1 for u in users.values() if u.get("role") == "admin")
        if admin_count <= 1:
            return jsonify({"success": False, "error": "Không thể xoá tài khoản admin duy nhất."}), 400

    # Prevent deleting own account
    if username == session.get("username"):
        return jsonify({"success": False, "error": "Không thể xoá tài khoản của chính mình."}), 400

    del users[username]
    save_users_config(users)
    return jsonify({"success": True, "message": f'Đã xoá tài khoản "{username}".'})


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
