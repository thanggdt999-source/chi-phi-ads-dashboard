from flask import Flask, render_template, render_template_string, request, jsonify, session, redirect, url_for
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
MONTHLY_SHEETS_ROOT = Path(os.getenv("MONTHLY_SHEETS_ROOT", str(Path(__file__).parent.parent / "storage" / "monthly_sheets")))
STATIC_ASSET_VERSION = os.getenv("STATIC_ASSET_VERSION", str(int(datetime.now().timestamp())))

ROLE_LEVELS = {"admin": 3, "lead": 2, "employee": 1}
TEAM_CODES = ["TEAM_1", "TEAM_2", "TEAM_3", "TEAM_4", "TEAM_5", "Fanmen"]
LOGIN_BOARD_NAME = os.getenv("LOGIN_BOARD_NAME", "Chi Phí Ads Realtime | GDT GROUP")


@app.context_processor
def inject_asset_version():
    return {"asset_version": STATIC_ASSET_VERSION}

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


def is_valid_sheet_url(sheet_url: str) -> bool:
    return bool(sheet_url) and bool(extract_sheet_id(sheet_url))


def normalize_month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def current_month_key() -> str:
    now = datetime.now()
    return normalize_month_key(now.year, now.month)


def month_label(month_key: str) -> str:
    try:
        year_str, month_str = month_key.split("-", 1)
        return f"Tháng {int(month_str)}/{int(year_str)}"
    except Exception:
        return month_key


def detect_month_key_from_text(text: str) -> str:
    value = (text or "").lower().strip()
    now = datetime.now()

    # e.g. "thang 5 2026" / "tháng 5"
    m = re.search(r"(?:th[aá]ng)\s*(1[0-2]|0?[1-9])(?:\D+(20\d{2}))?", value)
    if m:
        month = int(m.group(1))
        year = int(m.group(2)) if m.group(2) else now.year
        return normalize_month_key(year, month)

    # e.g. 05/2026, 5-2026
    m = re.search(r"\b(0?[1-9]|1[0-2])[/-](20\d{2})\b", value)
    if m:
        return normalize_month_key(int(m.group(2)), int(m.group(1)))

    # e.g. 2026-05
    m = re.search(r"\b(20\d{2})[/-](0?[1-9]|1[0-2])\b", value)
    if m:
        return normalize_month_key(int(m.group(1)), int(m.group(2)))

    return current_month_key()


def get_sheet_name_and_month(sheet_url: str) -> tuple[str, str, str]:
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return "", current_month_key(), ""

    clean_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    sheet_name = sheet_id
    month_key = current_month_key()
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        sheet_name = spreadsheet.title or sheet_id
        month_key = detect_month_key_from_text(sheet_name)
    except Exception:
        # Keep fallback values if title cannot be fetched
        pass

    return sheet_name, month_key, clean_url


def save_monthly_sheet_record(username: str, sheet_url: str, sheet_name: str, month_key: str) -> None:
    month_dir = MONTHLY_SHEETS_ROOT / month_key
    month_dir.mkdir(parents=True, exist_ok=True)
    user_file = month_dir / f"{username}.json"

    payload = {
        "username": username,
        "month": month_key,
        "entries": [],
    }
    if user_file.exists():
        try:
            with user_file.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                payload.update(loaded)
        except Exception:
            pass

    entries = payload.get("entries")
    if not isinstance(entries, list):
        entries = []

    now_iso = datetime.now().isoformat(timespec="seconds")
    existing_idx = next((i for i, e in enumerate(entries) if e.get("sheet_url") == sheet_url), -1)
    record = {
        "sheet_name": sheet_name,
        "sheet_url": sheet_url,
        "saved_at": now_iso,
    }
    if existing_idx >= 0:
        entries[existing_idx] = record
    else:
        entries.append(record)

    payload["entries"] = entries
    with user_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def get_user_monthly_sheets(username: str, fallback_sheet_url: str = "") -> list:
    result = []
    if MONTHLY_SHEETS_ROOT.exists():
        for month_dir in MONTHLY_SHEETS_ROOT.iterdir():
            if not month_dir.is_dir():
                continue
            month_key = month_dir.name
            if not re.match(r"^\d{4}-\d{2}$", month_key):
                continue
            user_file = month_dir / f"{username}.json"
            if not user_file.exists():
                continue
            try:
                with user_file.open("r", encoding="utf-8") as f:
                    payload = json.load(f)
                entries = payload.get("entries", []) if isinstance(payload, dict) else []
                if not entries:
                    continue
                latest = entries[-1]
                result.append({
                    "month_key": month_key,
                    "month_label": month_label(month_key),
                    "sheet_name": latest.get("sheet_name", ""),
                    "sheet_url": latest.get("sheet_url", ""),
                    "folder_url": url_for("view_month_folder", month_key=month_key),
                })
            except Exception:
                continue

    result.sort(key=lambda x: x.get("month_key", ""), reverse=True)

    if fallback_sheet_url and not any(item.get("sheet_url") == fallback_sheet_url for item in result):
        mk = current_month_key()
        result.insert(0, {
            "month_key": mk,
            "month_label": month_label(mk),
            "sheet_name": "Sheet hiện tại",
            "sheet_url": fallback_sheet_url,
            "folder_url": url_for("view_month_folder", month_key=mk),
        })

    return result


def set_session_user(username: str, user: dict, *, elevated: bool = False) -> None:
    session["logged_in"] = True
    session["username"] = username
    session["role"] = user.get("role", "employee")
    session["team"] = user.get("team", "")
    session["display_name"] = user.get("display_name", username)
    session["sheet_url"] = user.get("sheet_url", "")
    session["is_elevated"] = elevated
    if user.get("role") == "employee" and not elevated:
        session["base_employee"] = {
            "username": username,
            "display_name": user.get("display_name", username),
            "team": user.get("team", ""),
            "sheet_url": user.get("sheet_url", ""),
        }


def get_base_employee_session() -> dict:
    base = session.get("base_employee")
    return base if isinstance(base, dict) else {}


def restore_base_employee_session() -> bool:
    base = get_base_employee_session()
    if not base:
        return False

    base_username = base.get("username", "").strip()
    users = load_users_config()
    user = users.get(base_username)
    if not user or user.get("role") != "employee":
        return False

    set_session_user(base_username, user, elevated=False)
    return True


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
def root_redirect():
    return redirect(url_for("index"))


@app.route("/dashboard")
@login_required
def index():
    username = session.get("username", "")
    role = session.get("role", "employee")
    display_name = session.get("display_name", username)
    team = session.get("team", "")
    sheet_url = session.get("sheet_url", "")
    is_elevated = bool(session.get("is_elevated", False))
    base_employee = get_base_employee_session()
    can_elevate = role == "employee" and not is_elevated
    can_view_team = role in {"lead", "admin"}
    can_manage_users = role == "admin"
    accessible_sheets = get_accessible_sheets_for_user(username)
    monthly_sheets = get_user_monthly_sheets(username, fallback_sheet_url=sheet_url)
    return render_template(
        "index.html",
        role=role,
        display_name=display_name,
        team=team,
        sheet_url=sheet_url,
        is_elevated=is_elevated,
        base_employee=base_employee,
        can_elevate=can_elevate,
        can_view_team=can_view_team,
        can_manage_users=can_manage_users,
        accessible_sheets_count=len(accessible_sheets),
        accessible_sheets_json=json.dumps(accessible_sheets, ensure_ascii=False),
        monthly_sheets_json=json.dumps(monthly_sheets, ensure_ascii=False),
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if is_logged_in():
            return redirect(url_for("index"))
        success_message = ""
        registered = request.args.get("registered", "").strip()
        registered_username = request.args.get("username", "").strip()
        if registered == "1":
            success_message = (
                f'Đăng ký thành công cho tài khoản "{registered_username}". Vui lòng đăng nhập để vào hệ thống.'
                if registered_username
                else "Đăng ký thành công. Vui lòng đăng nhập để vào hệ thống."
            )
        return render_template(
            "login.html",
            error="",
            success=success_message,
            title="Chi Phi Ads Dashboard",
            subtitle="Ae làm cái này thì sau không cần điền chi phí ads mỗi ngày nữa nè. Phêêêêêêêê...!",
            board_name=LOGIN_BOARD_NAME,
            form_action=url_for("login"),
            submit_label="Đăng nhập",
            show_register_link=True,
            show_back_link=False,
            mode="employee",
        )

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    user = get_user(username)

    if user and user.get("password") == password and user.get("role") == "employee":
        set_session_user(username, user, elevated=False)
        next_url = request.args.get("next") or url_for("index")
        return redirect(next_url)

    error_message = "Sai tài khoản hoặc mật khẩu"

    if not user:
        error_message = "Tài khoản chưa tồn tại trong hệ thống"
    elif user.get("role") != "employee" and user.get("password") == password:
        error_message = "Tài khoản này là Leader/Admin. Vui lòng đăng nhập bằng tài khoản nhân viên ở bước 1 trước."
    elif user.get("role") != "employee":
        error_message = "Bước 1 chỉ dùng tài khoản nhân viên"

    return render_template(
        "login.html",
        error=error_message,
        success="",
        title="Chi Phi Ads Dashboard",
        subtitle="Ae làm cái này thì sau không cần điền chi phí ads mỗi ngày nữa nè. Phêêêêêêêê...!",
        board_name=LOGIN_BOARD_NAME,
        form_action=url_for("login"),
        submit_label="Đăng nhập",
        show_register_link=True,
        show_back_link=False,
        mode="employee",
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
            success="",
            title="Đăng nhập Leader / Admin",
            subtitle="Bước 2/2: Nâng quyền để xem dữ liệu team hoặc toàn hệ thống.",
            board_name=LOGIN_BOARD_NAME,
            form_action=url_for("privileged_login"),
            submit_label="Mở quyền xem tổng",
            show_register_link=False,
            show_back_link=True,
            mode="privileged",
        )

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    user = get_user(username)

    if user and user.get("password") == password and user.get("role") in {"lead", "admin"}:
        set_session_user(username, user, elevated=True)
        return redirect(url_for("index"))

    return render_template(
        "login.html",
        error="Bước 2 chỉ chấp nhận tài khoản Leader hoặc Admin hợp lệ.",
        success="",
        title="Đăng nhập Leader / Admin",
        subtitle="Bước 2/2: Nâng quyền để xem dữ liệu team hoặc toàn hệ thống.",
        board_name=LOGIN_BOARD_NAME,
        form_action=url_for("privileged_login"),
        submit_label="Mở quyền xem tổng",
        show_register_link=False,
        show_back_link=True,
        mode="privileged",
    )


@app.route("/return-to-employee", methods=["POST"])
@login_required
def return_to_employee():
    if not bool(session.get("is_elevated", False)):
        return redirect(url_for("index"))

    if not restore_base_employee_session():
        session.clear()
        return redirect(url_for("login"))

    return redirect(url_for("index"))


@app.route("/register", methods=["GET", "POST"])
def register_employee():
    if is_logged_in():
        return redirect(url_for("index"))

    form_values = {
        "username": "",
        "display_name": "",
        "team": "",
        "sheet_url": "",
    }

    if request.method == "GET":
        return render_template(
            "register.html",
            error="",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    username = request.form.get("username", "").strip()
    display_name = request.form.get("display_name", "").strip()
    team = request.form.get("team", "").strip()
    sheet_url = request.form.get("sheet_url", "").strip()
    password = request.form.get("password", "")
    confirm_password = request.form.get("confirm_password", "")

    form_values = {
        "username": username,
        "display_name": display_name,
        "team": team,
        "sheet_url": sheet_url,
    }

    if not username or not display_name or not password or not confirm_password or not team or not sheet_url:
        return render_template(
            "register.html",
            error="Vui lòng nhập đầy đủ thông tin đăng ký.",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    if password != confirm_password:
        return render_template(
            "register.html",
            error="Mật khẩu xác nhận không khớp.",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    if team not in TEAM_CODES:
        return render_template(
            "register.html",
            error="Vui lòng chọn team hợp lệ.",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    if not is_valid_sheet_url(sheet_url):
        return render_template(
            "register.html",
            error="Link Google Sheet không hợp lệ.",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    users = load_users_config()
    if username in users:
        return render_template(
            "register.html",
            error=f'Tên đăng nhập "{username}" đã tồn tại.',
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    sheet_name, month_key, clean_url = get_sheet_name_and_month(sheet_url)
    users[username] = {
        "password": password,
        "role": "employee",
        "team": team,
        "display_name": display_name,
        "sheet_url": clean_url or sheet_url,
    }
    save_users_config(users)
    if clean_url:
        save_monthly_sheet_record(username, clean_url, sheet_name or username, month_key)

    return redirect(url_for("login", registered="1", username=username))


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
    if role == "employee" and not sheet_url:
        return jsonify({"success": False, "error": "Nhân viên bắt buộc phải có link Google Sheet."}), 400

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
        sheet_name, month_key, clean_url = get_sheet_name_and_month(sheet_url)
        users[username]["sheet_url"] = clean_url or sheet_url
        if clean_url:
            save_monthly_sheet_record(username, clean_url, sheet_name or username, month_key)

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
    team = data.get("team", users[username].get("team", "")).strip()
    display_name = data.get("display_name", users[username].get("display_name", username)).strip()
    sheet_url = data.get("sheet_url", users[username].get("sheet_url", "")).strip()
    new_password = data.get("password", "").strip()

    if role not in ROLE_LEVELS:
        return jsonify({"success": False, "error": "Vai trò không hợp lệ."}), 400
    if role in ("lead", "employee") and team not in TEAM_CODES:
        return jsonify({"success": False, "error": "Vui lòng chọn team hợp lệ."}), 400
    if role == "employee" and not sheet_url:
        return jsonify({"success": False, "error": "Nhân viên bắt buộc phải có link Google Sheet."}), 400

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
        sheet_name, month_key, clean_url = get_sheet_name_and_month(sheet_url)
        users[username]["sheet_url"] = clean_url or sheet_url
        if clean_url:
            save_monthly_sheet_record(username, clean_url, sheet_name or username, month_key)
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

    username = session.get("username", "")
    role = session.get("role", "employee")
    sheet_name, month_key, clean_url = get_sheet_name_and_month(sheet_url)
    if not clean_url:
        return jsonify({"success": False, "error": "URL không hợp lệ"}), 400

    # Đọc file hiện tại
    existing_lines = []
    if SHEET_URLS_PATH.exists():
        with open(SHEET_URLS_PATH, "r", encoding="utf-8") as f:
            existing_lines = [l.strip() for l in f if l.strip()]

    already_exists = any(sheet_id in l for l in existing_lines)
    if not already_exists:
        with open(SHEET_URLS_PATH, "a", encoding="utf-8") as f:
            f.write(f"{sheet_name},{clean_url}\n")

    if username:
        save_monthly_sheet_record(username, clean_url, sheet_name, month_key)

        if role == "employee":
            users = load_users_config()
            if username in users:
                users[username]["sheet_url"] = clean_url
                save_users_config(users)
            session["sheet_url"] = clean_url

    msg = f'Đã lưu sheet "{sheet_name}" vào thư mục tháng {month_label(month_key)}.'
    return jsonify({
        "success": True,
        "message": msg,
        "name": sheet_name,
        "already_exists": already_exists,
        "month_key": month_key,
        "month_label": month_label(month_key),
        "clean_url": clean_url,
        "folder_url": url_for("view_month_folder", month_key=month_key),
    })


@app.route("/monthly-folder/<month_key>", methods=["GET"])
@login_required
def view_month_folder(month_key):
    if not re.match(r"^\d{4}-\d{2}$", month_key):
        return "Tháng không hợp lệ.", 400

    username = session.get("username", "")
    user_file = MONTHLY_SHEETS_ROOT / month_key / f"{username}.json"
    entries = []
    if user_file.exists():
        try:
            with user_file.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            raw_entries = payload.get("entries", []) if isinstance(payload, dict) else []
            if isinstance(raw_entries, list):
                entries = raw_entries
        except Exception:
            entries = []

    return render_template_string(
        """
        <!DOCTYPE html>
        <html lang="vi">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Thư mục tháng {{ month_label }}</title>
            <style>
                body { font-family: Segoe UI, sans-serif; margin: 24px; background: #f8fafc; color: #0f172a; }
                .card { background: #fff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 16px; max-width: 980px; }
                h1 { margin: 0 0 8px; font-size: 1.4rem; }
                table { width: 100%; border-collapse: collapse; margin-top: 12px; }
                th, td { border-bottom: 1px solid #e2e8f0; text-align: left; padding: 10px 8px; }
                th { background: #f1f5f9; }
                a { color: #2563eb; }
            </style>
        </head>
        <body>
            <div class="card">
                <h1>Thư mục sheet tháng {{ month_label }}</h1>
                <div>Người dùng: <strong>{{ username }}</strong></div>
                {% if entries %}
                <table>
                    <thead>
                        <tr>
                            <th>Tên sheet</th>
                            <th>URL</th>
                            <th>Lưu lúc</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for e in entries %}
                        <tr>
                            <td>{{ e.get('sheet_name', '-') }}</td>
                            <td><a href="{{ e.get('sheet_url', '#') }}" target="_blank">Mở sheet</a></td>
                            <td>{{ e.get('saved_at', '-') }}</td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
                {% else %}
                <p>Chưa có sheet nào được lưu cho tháng này.</p>
                {% endif %}
            </div>
        </body>
        </html>
        """,
        month_label=month_label(month_key),
        username=username,
        entries=entries,
    )

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
