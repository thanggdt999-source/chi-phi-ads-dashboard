import argparse
import html
import secrets
import sys
import tempfile
import unicodedata

from flask import Flask, render_template, render_template_string, request, jsonify, session, redirect, url_for
from google.oauth2.service_account import Credentials
import gspread
import re
import os
from pathlib import Path
from functools import wraps
from datetime import date, datetime, timedelta
import json
from typing import Optional
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from zoneinfo import ZoneInfo

try:
    import psycopg2
except Exception:
    psycopg2 = None

app = Flask(__name__)
app.secret_key = os.getenv("WEB_APP_SECRET_KEY", "change-this-secret-in-production")
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "storage" / "credentials" / "service_account.json"
WEB_STATIC_DIR = Path(__file__).parent / "static"
SHEET_URLS_PATH = Path(os.getenv("SHEET_URLS_PATH", str(Path(__file__).parent.parent / "storage" / "sheet_urls.csv")))
AUTO_STATE_PATH = Path(os.getenv("AUTO_STATE_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "auto_fill_state.json")))
USERS_FILE_PATH = Path(os.getenv("USERS_FILE_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "users.json")))
USERS_FILE_BACKUP_PATH = Path(os.getenv("USERS_FILE_BACKUP_PATH", str(Path(__file__).parent.parent / "storage" / "config" / "users.backup.json")))
LEGACY_USERS_FILE_PATH = Path(os.getenv("LEGACY_USERS_FILE_PATH", str(Path(__file__).parent.parent / "storage" / "users.json")))
USERS_DATABASE_URL = (os.getenv("USERS_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
MONTHLY_SHEETS_ROOT = Path(os.getenv("MONTHLY_SHEETS_ROOT", str(Path(__file__).parent.parent / "storage" / "monthly_sheets")))
STATIC_ASSET_VERSION = os.getenv("STATIC_ASSET_VERSION", str(int(datetime.now().timestamp())))
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
INTERNAL_CRON_SECRET = os.getenv("INTERNAL_CRON_SECRET", "").strip()
TELEGRAM_REPORT_ENDPOINT = os.getenv(
    "TELEGRAM_REPORT_ENDPOINT",
    "https://ads.hexistoree.click/internal/telegram/reports/run",
).strip()
TELEGRAM_REPORT_STATE_PATH = Path(
    os.getenv(
        "TELEGRAM_REPORT_STATE_PATH",
        str(Path(__file__).parent.parent / "storage" / "config" / "telegram_report_state.json"),
    )
)
TELEGRAM_REPORT_TIMEZONE = os.getenv("TELEGRAM_REPORT_TIMEZONE", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
TELEGRAM_REPORT_START_HOUR = int(os.getenv("TELEGRAM_REPORT_START_HOUR", "6"))
TELEGRAM_REPORT_START_MINUTE = max(0, min(59, int(os.getenv("TELEGRAM_REPORT_START_MINUTE", "30"))))
TELEGRAM_REPORT_END_HOUR = int(os.getenv("TELEGRAM_REPORT_END_HOUR", "22"))
TELEGRAM_REPORT_END_MINUTE = max(0, min(59, int(os.getenv("TELEGRAM_REPORT_END_MINUTE", "30"))))
TELEGRAM_REPORT_INTERVAL_MINUTES = max(1, min(60, int(os.getenv("TELEGRAM_REPORT_INTERVAL_MINUTES", "5"))))
TELEGRAM_REPORT_MAX_PRODUCTS = max(1, int(os.getenv("TELEGRAM_REPORT_MAX_PRODUCTS", "8")))
SESSION_TIMEOUT_SECONDS = int(os.getenv("SESSION_TIMEOUT_SECONDS", "600"))  # 10 minutes
META_GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", "v20.0").strip() or "v20.0"
META_ACCESS_TOKEN_PATH = Path(
    os.getenv(
        "META_ACCESS_TOKEN_PATH",
        str(Path(__file__).parent.parent / "storage" / "credentials" / "meta_access_token.json"),
    )
)
META_TOKEN_VAULT_PATH = Path(
    os.getenv(
        "META_TOKEN_VAULT_PATH",
        str(Path(__file__).parent.parent / "storage" / "config" / "meta_token_vault.json"),
    )
)
CHI_PHI_ADS_SHEET = "Chi phí ADS"
SHEET_DATA_START_ROW = 3
MAX_EMPTY_STREAK_TO_STOP_SCAN = 120

ROLE_LEVELS = {"admin": 3, "lead": 2, "employee": 1}
TEAM_CODES = ["TEAM_1", "TEAM_2", "TEAM_3", "TEAM_4", "TEAM_5", "Fanmen"]
LOGIN_BOARD_NAME = os.getenv("LOGIN_BOARD_NAME", "Chi Phí Ads Realtime | GDT GROUP")
BUILTIN_ADMIN_USERNAME = (os.getenv("BUILTIN_ADMIN_USERNAME", "admin") or "admin").strip().lower() or "admin"
BUILTIN_ADMIN_PASSWORD = (os.getenv("BUILTIN_ADMIN_PASSWORD", "admin") or "admin").strip() or "admin"
BUILTIN_ADMIN_TELEGRAM_BOT_TOKEN = (
    os.getenv("BUILTIN_ADMIN_TELEGRAM_BOT_TOKEN", "8262654965:AAGr2tBaw8WkXsByQl750TTEqj5gEg1H9sE") or ""
).strip()
BUILTIN_ADMIN_TELEGRAM_BOT_USERNAME = (
    os.getenv("BUILTIN_ADMIN_TELEGRAM_BOT_USERNAME", "Thanggdt00011_bot") or ""
).strip().lstrip("@")
BUILTIN_ADMIN_TELEGRAM_CHAT_ID = (os.getenv("BUILTIN_ADMIN_TELEGRAM_CHAT_ID", "6483090920") or "").strip()
BUILTIN_ADMIN_TELEGRAM_USERNAME = (
    os.getenv("BUILTIN_ADMIN_TELEGRAM_USERNAME", "kimtan18t") or ""
).strip().lstrip("@")
BUILTIN_ADMIN_SHEET_URL = (
    os.getenv(
        "BUILTIN_ADMIN_SHEET_URL",
        "https://docs.google.com/spreadsheets/d/1jUXxwbFIIYJIHVNaVlIIHbPIsu_mPvnGhsTf6eukEWE/edit?gid=1185483623#gid=1185483623",
    )
    or ""
).strip()
BUILTIN_ADMIN_PERFORMANCE_SHEET_URL = (
    os.getenv(
        "BUILTIN_ADMIN_PERFORMANCE_SHEET_URL",
        "https://docs.google.com/spreadsheets/d/1z8UUQtt1UHzbgmZH9yNxJXwIt35a-fhY1n4k__ceWwQ/edit?gid=369723317#gid=369723317",
    )
    or ""
).strip()


@app.context_processor
def inject_asset_version():
    return {"asset_version": STATIC_ASSET_VERSION}


def read_web_static_asset(filename: str) -> str:
    try:
        path = WEB_STATIC_DIR / filename
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


@app.before_request
def enforce_session_timeout():
    if session.get("logged_in") is not True:
        return None

    if is_session_timeout():
        session.clear()
        if (request.path or "").startswith("/api/"):
            return jsonify({
                "success": False,
                "error": "Phiên đăng nhập đã hết hạn do không hoạt động trong 10 phút. Vui lòng đăng nhập lại.",
                "login_url": url_for("login", expired="1"),
            }), 401
        return redirect(url_for("login", next=request.path, expired="1"))

    session["last_activity"] = datetime.now().timestamp()
    session.modified = True
    return None


@app.after_request
def disable_cache_for_web_assets(response):
    path = request.path or ""
    content_type = (response.content_type or "").lower()

    should_disable_cache = path.startswith("/static/") or content_type.startswith("text/html")
    if should_disable_cache:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

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
    atomic_write_json_file(USERS_FILE_PATH, users)
    atomic_write_json_file(USERS_FILE_BACKUP_PATH, users)


def load_json_dict_file(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) and payload else None
    except Exception:
        return None


def atomic_write_json_file(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass


def _is_users_db_enabled() -> bool:
    return bool(USERS_DATABASE_URL) and psycopg2 is not None


def _ensure_users_db_table() -> bool:
    if not _is_users_db_enabled():
        return False
    try:
        with psycopg2.connect(USERS_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_users_store (
                        store_key TEXT PRIMARY KEY,
                        users_json JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            conn.commit()
        return True
    except Exception:
        return False


def _load_users_from_db() -> Optional[dict]:
    if not _is_users_db_enabled():
        return None
    if not _ensure_users_db_table():
        return None
    try:
        with psycopg2.connect(USERS_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT users_json FROM app_users_store WHERE store_key = %s", ("default",))
                row = cur.fetchone()
        if not row:
            return None
        payload = row[0]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return payload if isinstance(payload, dict) and payload else None
    except Exception:
        return None


def _save_users_to_db(users: dict) -> bool:
    if not _is_users_db_enabled():
        return False
    if not _ensure_users_db_table():
        return False
    try:
        with psycopg2.connect(USERS_DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO app_users_store (store_key, users_json, updated_at)
                    VALUES (%s, %s::jsonb, NOW())
                    ON CONFLICT (store_key)
                    DO UPDATE SET users_json = EXCLUDED.users_json, updated_at = NOW()
                    """,
                    ("default", json.dumps(users, ensure_ascii=False)),
                )
            conn.commit()
        return True
    except Exception:
        return False


def _load_users_from_file_layers(users_from_env: dict) -> dict:
    """Legacy loader stack (env/file/backup/auto-generated)."""
    users_from_file = load_json_dict_file(USERS_FILE_PATH)
    users_from_legacy = load_json_dict_file(LEGACY_USERS_FILE_PATH)
    if users_from_file:
        merged = dict(users_from_env)
        # Migration-safe precedence: env baseline < legacy snapshot < current file.
        # The current users file is authoritative and must not be overridden by legacy data.
        if users_from_legacy:
            merged.update(users_from_legacy)
        merged.update(users_from_file)
        if users_from_legacy:
            try:
                atomic_write_json_file(USERS_FILE_PATH, merged)
                atomic_write_json_file(USERS_FILE_BACKUP_PATH, merged)
            except Exception:
                pass
        elif users_from_file != merged:
            try:
                atomic_write_json_file(USERS_FILE_BACKUP_PATH, merged)
            except Exception:
                pass
        # Keep backup in sync so we can recover if the main file is damaged later.
        try:
            atomic_write_json_file(USERS_FILE_BACKUP_PATH, merged)
        except Exception:
            pass
        return merged

    # Migration fallback: older releases stored users in storage/users.json.
    if users_from_legacy:
        merged = dict(users_from_env)
        merged.update(users_from_legacy)
        try:
            atomic_write_json_file(USERS_FILE_PATH, merged)
            atomic_write_json_file(USERS_FILE_BACKUP_PATH, merged)
        except Exception:
            pass
        return merged

    # Recover from backup if main file is corrupted/empty.
    users_from_backup = load_json_dict_file(USERS_FILE_BACKUP_PATH)
    if users_from_backup:
        if users_from_env:
            merged = dict(users_from_env)
            merged.update(users_from_backup)
            try:
                atomic_write_json_file(USERS_FILE_PATH, merged)
            except Exception:
                pass
            return merged
        try:
            atomic_write_json_file(USERS_FILE_PATH, users_from_backup)
        except Exception:
            pass
        return users_from_backup

    if users_from_env:
        return users_from_env

    # Only auto-generate defaults when users file does not exist yet.
    if USERS_FILE_PATH.exists():
        # Safety guard: never silently replace an existing user store with a
        # synthetic admin-only account. Returning empty config here preserves
        # data integrity and avoids the false impression that users were deleted.
        return dict(users_from_env) if users_from_env else {}

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


def load_users_config() -> dict:
    """Load user config from Postgres (preferred) then file layers."""
    users_from_env = {}
    config_json = os.getenv("USERS_CONFIG", "").strip()
    if config_json:
        try:
            parsed = json.loads(config_json)
            if isinstance(parsed, dict):
                users_from_env = parsed
        except Exception:
            pass

    users_from_db = _load_users_from_db()
    if users_from_db:
        merged = dict(users_from_env)
        merged.update(users_from_db)
        merged[BUILTIN_ADMIN_USERNAME] = ensure_builtin_admin_profile(merged.get(BUILTIN_ADMIN_USERNAME, {}))
        if merged != users_from_db:
            _save_users_to_db(merged)
        return merged

    loaded = _load_users_from_file_layers(users_from_env)
    loaded[BUILTIN_ADMIN_USERNAME] = ensure_builtin_admin_profile(loaded.get(BUILTIN_ADMIN_USERNAME, {}))
    if loaded:
        _save_users_to_db(loaded)
    return loaded


def ensure_builtin_admin_profile(raw_user: object) -> dict:
    builtin_admin = raw_user if isinstance(raw_user, dict) else {}
    builtin_admin["password"] = BUILTIN_ADMIN_PASSWORD
    builtin_admin["role"] = "admin"
    builtin_admin["display_name"] = builtin_admin.get("display_name") or "Built-in Admin"
    if BUILTIN_ADMIN_SHEET_URL:
        builtin_admin["sheet_url"] = BUILTIN_ADMIN_SHEET_URL
    if BUILTIN_ADMIN_PERFORMANCE_SHEET_URL:
        builtin_admin["performance_sheet_url"] = BUILTIN_ADMIN_PERFORMANCE_SHEET_URL

    default_chat_id = normalize_telegram_chat_id(BUILTIN_ADMIN_TELEGRAM_CHAT_ID)
    default_bot_username = normalize_telegram_bot_username(BUILTIN_ADMIN_TELEGRAM_BOT_USERNAME)
    default_bot_token = normalize_telegram_bot_token(BUILTIN_ADMIN_TELEGRAM_BOT_TOKEN)
    default_telegram_username = normalize_telegram_username(BUILTIN_ADMIN_TELEGRAM_USERNAME)

    if default_chat_id:
        builtin_admin.setdefault("telegram_chat_id", default_chat_id)
    if default_bot_username:
        builtin_admin.setdefault("telegram_bot_username", default_bot_username)
    if default_bot_token:
        builtin_admin.setdefault("telegram_bot_token", default_bot_token)
    if default_telegram_username:
        builtin_admin.setdefault("telegram_username", default_telegram_username)
    if default_chat_id and default_bot_username and default_bot_token:
        builtin_admin.setdefault("telegram_verified", True)
        builtin_admin.setdefault("telegram_test_status", "sent")
    return builtin_admin


def normalize_username(value: str) -> str:
    raw = unicodedata.normalize("NFKC", str(value or ""))
    raw = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", raw)
    compact = re.sub(r"\s+", "", raw).strip().lower()
    return compact


def get_user_entry(username: str, users: Optional[dict] = None) -> tuple[str, Optional[dict]]:
    source = users if isinstance(users, dict) else load_users_config()
    if not source:
        return "", None

    direct_key = str(username or "").strip()
    if direct_key in source:
        return direct_key, source.get(direct_key)

    target = normalize_username(username)
    if not target:
        return "", None

    for key, user in source.items():
        if normalize_username(key) == target:
            return key, user if isinstance(user, dict) else None

    # Friendly fallback: allow shorthand login without "emp_" prefix.
    # Example: thang -> emp_thang
    if not target.startswith("emp_"):
        alias = f"emp_{target}"
        for key, user in source.items():
            if normalize_username(key) == alias:
                return key, user if isinstance(user, dict) else None

    return "", None


def get_user(username: str) -> Optional[dict]:
    _, user = get_user_entry(username)
    return user


def is_valid_sheet_url(sheet_url: str) -> bool:
    return bool(sheet_url) and bool(extract_sheet_id(sheet_url))


def normalize_telegram_chat_id(value: str) -> str:
    chat_id = (value or "").strip()
    compact = re.sub(r"\s+", "", chat_id)

    # Telegram private/group chat IDs are numeric (group IDs can be negative).
    if re.fullmatch(r"-?\d{6,20}", compact):
        return compact

    # Support pasted text from helpers like @userinfobot (e.g. "ID: 123456789").
    match = re.search(r"-?\d{6,20}", chat_id)
    if match:
        return match.group(0)
    return ""


def normalize_telegram_username(value: str) -> str:
    username = (value or "").strip()
    if "/" in username:
        username = username.rstrip("/").split("/")[-1]
    username = username.lstrip("@")
    if not username:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
        return username
    return ""


def normalize_telegram_bot_username(value: str) -> str:
    return normalize_telegram_username(value)


def normalize_telegram_bot_token(value: str) -> str:
    token = (value or "").strip()
    if re.fullmatch(r"\d{6,12}:[A-Za-z0-9_-]{30,80}", token):
        return token
    return ""


def ensure_telegram_bind_code(actor_username: str, *, force_new: bool = False) -> str:
    actor_key = (actor_username or "").strip()
    if not actor_key:
        actor_key = "anonymous"

    session_key = f"telegram_bind_code::{actor_key}"
    current = str(session.get(session_key, "")).strip()
    if current and not force_new:
        return current

    code = f"cpads_{secrets.token_hex(4)}"
    session[session_key] = code
    return code


def telegram_bot_api_get(bot_token: str, method: str, params: Optional[dict] = None) -> tuple[bool, dict, str]:
    normalized_token = normalize_telegram_bot_token(bot_token)
    if not normalized_token:
        return False, {}, "Bot token không hợp lệ."

    query = urllib_parse.urlencode(params or {})
    endpoint = f"https://api.telegram.org/bot{normalized_token}/{method}"
    url = endpoint if not query else f"{endpoint}?{query}"
    req = urllib_request.Request(url=url, method="GET")

    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            payload = json.loads(body) if body else {}
            if payload.get("ok"):
                return True, payload, "OK"
            return False, payload, payload.get("description", "Telegram từ chối yêu cầu.")
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(body) if body else {}
            return False, payload, payload.get("description", f"Telegram HTTP {exc.code}")
        except Exception:
            return False, {}, f"Telegram HTTP {exc.code}"
    except Exception:
        return False, {}, "Không thể kết nối Telegram lúc này."


def resolve_bot_username_from_token(bot_token: str) -> tuple[bool, str, str]:
    ok, payload, err = telegram_bot_api_get(bot_token, "getMe")
    if not ok:
        return False, "", err

    username = normalize_telegram_bot_username(str((payload.get("result") or {}).get("username", "")))
    if not username:
        return False, "", "Không đọc được username bot từ token này."
    return True, username, "OK"


def find_chat_from_bind_code(bot_token: str, bind_code: str) -> tuple[bool, dict, str]:
    if not bind_code:
        return False, {}, "Thiếu mã liên kết Telegram."

    ok, payload, err = telegram_bot_api_get(bot_token, "getUpdates", {"limit": 100, "timeout": 0})
    if not ok:
        return False, {}, err

    for update in reversed(payload.get("result", [])):
        message = update.get("message") or {}
        text = str(message.get("text") or "").strip()
        if not text.startswith("/start"):
            continue
        compact_text = re.sub(r"\s+", " ", text)
        if bind_code not in compact_text:
            continue

        chat_id = normalize_telegram_chat_id(str((message.get("chat") or {}).get("id", "")))
        if not chat_id:
            continue

        telegram_username = normalize_telegram_username(str((message.get("from") or {}).get("username", "")))
        return True, {
            "chat_id": chat_id,
            "telegram_username": telegram_username,
        }, "OK"

    return False, {}, "Chưa tìm thấy tin nhắn /start đúng mã liên kết."


def is_telegram_report_role(user: Optional[dict]) -> bool:
    if not user:
        return False
    return str(user.get("role", "") or "").strip() in {"employee", "lead", "admin"}


def resolve_telegram_setup_bot(raw_bot_token: str = "", raw_bot_username: str = "") -> tuple[bool, str, str, str]:
    effective_token = normalize_telegram_bot_token(raw_bot_token) or normalize_telegram_bot_token(TELEGRAM_BOT_TOKEN)
    if not effective_token:
        return False, "", "", "Bot token chưa sẵn sàng. Hãy tạo bot bằng BotFather hoặc cấu hình bot hệ thống."

    resolve_ok, resolved_bot_username, resolve_error = resolve_bot_username_from_token(effective_token)
    if not resolve_ok:
        return False, "", "", resolve_error

    normalized_bot_username = normalize_telegram_bot_username(raw_bot_username)
    if normalized_bot_username and normalized_bot_username != resolved_bot_username:
        return False, "", "", "Bot username không khớp với token đã nhập. Hãy kiểm tra lại bot token."

    return True, effective_token, resolved_bot_username, "OK"


def user_requires_telegram_setup(username: str, user: Optional[dict] = None) -> bool:
    target_user = user or get_user(username)
    if not is_telegram_report_role(target_user):
        return False

    role = str(target_user.get("role", "") or "").strip()

    chat_id = normalize_telegram_chat_id(str(target_user.get("telegram_chat_id", "")))
    if not chat_id:
        return True

    bot_username = normalize_telegram_bot_username(str(target_user.get("telegram_bot_username", ""))) or normalize_telegram_bot_username(TELEGRAM_BOT_USERNAME)
    bot_token = normalize_telegram_bot_token(str(target_user.get("telegram_bot_token", ""))) or normalize_telegram_bot_token(TELEGRAM_BOT_TOKEN)
    if not bot_username or not bot_token:
        return True

    # Backward compatibility: old users may not have verified flags but already have a working chat_id.
    if "telegram_verified" not in target_user and "telegram_test_status" not in target_user:
        return False

    if role in {"lead", "admin"}:
        return False

    if target_user.get("telegram_test_status") == "sent":
        return False
    return not bool(target_user.get("telegram_verified", False))


def employee_requires_telegram_setup(username: str, user: Optional[dict] = None) -> bool:
    return user_requires_telegram_setup(username, user)


def save_user_telegram_setup(
    username: str,
    *,
    chat_id: str,
    telegram_username: str,
    bot_username: str,
    bot_token: str,
    test_status: str,
) -> tuple[bool, dict]:
    users = load_users_config()
    user = users.get(username)
    if not is_telegram_report_role(user):
        return False, {}

    user["telegram_chat_id"] = chat_id
    if telegram_username:
        user["telegram_username"] = telegram_username
    else:
        user.pop("telegram_username", None)

    user["telegram_bot_username"] = bot_username
    # Never persist system token into user profile. Keep personal token only when explicitly provided.
    if normalize_telegram_bot_token(bot_token):
        user["telegram_bot_token"] = bot_token
    else:
        user.pop("telegram_bot_token", None)

    user["telegram_verified"] = test_status == "sent"
    user["telegram_test_status"] = test_status
    user["telegram_last_test_at"] = datetime.now().isoformat(timespec="seconds")
    save_users_config(users)
    return True, user


def save_employee_telegram_setup(
    username: str,
    *,
    chat_id: str,
    telegram_username: str,
    bot_username: str,
    bot_token: str,
    test_status: str,
) -> tuple[bool, dict]:
    return save_user_telegram_setup(
        username,
        chat_id=chat_id,
        telegram_username=telegram_username,
        bot_username=bot_username,
        bot_token=bot_token,
        test_status=test_status,
    )


def get_safe_next_url(raw_next: str) -> str:
    next_url = (raw_next or "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return next_url
    return url_for("index")


def get_notification_now() -> datetime:
    try:
        return datetime.now(ZoneInfo(TELEGRAM_REPORT_TIMEZONE))
    except Exception:
        return datetime.now()


def is_notification_window(now: datetime) -> bool:
    start_at = now.replace(
        hour=TELEGRAM_REPORT_START_HOUR,
        minute=TELEGRAM_REPORT_START_MINUTE,
        second=0,
        microsecond=0,
    )
    end_at = now.replace(
        hour=TELEGRAM_REPORT_END_HOUR,
        minute=TELEGRAM_REPORT_END_MINUTE,
        second=59,
        microsecond=999999,
    )
    if end_at < start_at:
        return False
    return start_at <= now <= end_at


def get_notification_slot(now: datetime) -> str:
    slot_minute = (now.minute // TELEGRAM_REPORT_INTERVAL_MINUTES) * TELEGRAM_REPORT_INTERVAL_MINUTES
    return now.replace(minute=slot_minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def load_telegram_report_state() -> dict:
    TELEGRAM_REPORT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not TELEGRAM_REPORT_STATE_PATH.exists():
        return {}

    try:
        with TELEGRAM_REPORT_STATE_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def save_telegram_report_state(state: dict) -> None:
    TELEGRAM_REPORT_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TELEGRAM_REPORT_STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def parse_row_date(value: str) -> Optional[date]:
    raw = (value or "").strip()
    if not raw:
        return None

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def send_telegram_message(chat_id: str, text: str, bot_token: str = "") -> tuple[bool, str]:
    effective_token = normalize_telegram_bot_token(bot_token) or normalize_telegram_bot_token(TELEGRAM_BOT_TOKEN)
    if not effective_token:
        return False, "Bot token chưa được cấu hình hợp lệ (cả token cá nhân và token hệ thống)."
    if not chat_id:
        return False, "Thiếu Telegram Chat ID."

    payload = json.dumps({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib_request.Request(
        url=f"https://api.telegram.org/bot{effective_token}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            parsed = json.loads(body) if body else {}
            if parsed.get("ok"):
                return True, "OK"
            return False, parsed.get("description", "Telegram từ chối gửi tin nhắn.")
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(body) if body else {}
            return False, parsed.get("description", f"Telegram HTTP {exc.code}")
        except Exception:
            return False, f"Telegram HTTP {exc.code}"
    except Exception:
        return False, "Không thể kết nối Telegram lúc này."


def get_current_telegram_setup_actor() -> tuple[str, Optional[dict]]:
    if is_logged_in() and session.get("role") in {"employee", "lead", "admin"}:
        username = str(session.get("username", "")).strip()
        if not username:
            return "", None
        user = get_user(username)
        if is_telegram_report_role(user):
            return username, user

    pending_username = str(session.get("pending_telegram_setup", "")).strip()
    if pending_username:
        user = get_user(pending_username)
        if user and user.get("role") == "employee":
            return pending_username, user

    return "", None


def render_telegram_setup_page(
    *,
    error: str,
    username: str,
    display_name: str,
    form_values: dict,
    bind_code: str,
    form_action: str,
    back_url: str | None = None,
    back_text: str | None = None,
    submit_label: str | None = None,
    title_text: str | None = None,
):
    return render_template(
        "telegram_setup.html",
        error=error,
        username=username,
        display_name=display_name,
        form_values=form_values,
        bind_code=bind_code,
        form_action=form_action,
        back_url=back_url,
        back_text=back_text,
        submit_label=submit_label,
        title_text=title_text,
    )


def send_telegram_test_message(chat_id: str, display_name: str, bot_token: str = "") -> tuple[str, str]:
    text = (
        f"Dạ thưa kính bẩm đại ca {display_name}!\n\n"
        "Em xin phép gửi tin nhắn test từ hệ thống Chi Phí Ads Dashboard.\n"
        "Nếu đại ca nhận được tin này, em sẽ tiếp tục gửi báo cáo tự động đúng lịch về Telegram này."
    )
    effective_token = normalize_telegram_bot_token(bot_token)
    if not effective_token:
        return "not_configured", "Bot token chưa hợp lệ hoặc còn thiếu."

    ok, message = send_telegram_message(chat_id, text, bot_token=effective_token)
    if ok:
        return "sent", "Đã gửi tin nhắn test Telegram thành công."
    return "failed", message


def aggregate_product_metrics(rows: list) -> list:
    product_map = {}
    for row in rows:
        product_name = (row.get("Tên sản phẩm - VN") or "Chưa rõ sản phẩm").strip() or "Chưa rõ sản phẩm"
        entry = product_map.setdefault(product_name, {"name": product_name, "spend": 0.0, "data": 0})
        entry["spend"] += parse_spend(row.get("Số tiền chi tiêu - VND", ""))
        entry["data"] += parse_int(row.get("Số Data", ""))

    products = []
    for item in product_map.values():
        spend = round(item["spend"])
        data = item["data"]
        products.append({
            "name": item["name"],
            "spend": spend,
            "data": data,
            "cost_per_data": round(spend / data) if data > 0 else 0,
        })

    products.sort(key=lambda item: (item["spend"], item["data"]), reverse=True)
    return products


def build_advice_lines(summary: dict, products: list) -> list:
    advice = []
    total_spend = summary.get("total_spend", 0)
    total_data = summary.get("total_data", 0)
    average_cost = summary.get("cost_per_data", 0)

    if total_spend > 0 and total_data == 0:
        advice.append("Chi phí đã phát sinh nhưng chưa có data. Cần kiểm tra ads, form hoặc tracking ngay.")

    zero_data_products = [item for item in products if item["spend"] > 0 and item["data"] == 0]
    if zero_data_products:
        names = ", ".join(item["name"] for item in zero_data_products[:3])
        advice.append(f"Sản phẩm chưa ra data nhưng đã tiêu tiền: {html.escape(names)}.")

    expensive_products = [
        item for item in products
        if item["data"] > 0 and average_cost > 0 and item["cost_per_data"] >= average_cost * 1.4
    ]
    if expensive_products:
        top_item = expensive_products[0]
        advice.append(
            f"{html.escape(top_item['name'])} đang có chi phí/data cao hơn mặt bằng chung ({top_item['cost_per_data']:,} VND/data)."
        )

    if products:
        top_spend = products[0]
        advice.append(
            f"Sản phẩm tiêu nhiều nhất hiện tại: {html.escape(top_spend['name'])} ({top_spend['spend']:,} VND)."
        )

    if not advice:
        advice.append("Tạm thời chưa phát hiện bất thường rõ ràng. Sẽ bổ sung rule cảnh báo chi tiết ở bước train sau.")

    return advice[:3]


def build_employee_report_message(username: str, user: dict, now: datetime) -> tuple[bool, str, str]:
    sheet_url = (user.get("sheet_url") or "").strip()
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return False, "", "Sheet URL không hợp lệ."

    result = fetch_chi_phi_ads_data(sheet_id)
    if not result.get("success"):
        return False, "", result.get("error", "Không đọc được dữ liệu sheet.")

    today = now.date()
    rows = [row for row in result.get("data", []) if parse_row_date(row.get("Ngày", "")) == today]
    products = aggregate_product_metrics(rows)
    total_spend = round(sum(parse_spend(row.get("Số tiền chi tiêu - VND", "")) for row in rows))
    total_data = sum(parse_int(row.get("Số Data", "")) for row in rows)
    cost_per_data = round(total_spend / total_data) if total_data > 0 else 0
    ads_percent = (result.get("ads_percent") or "").strip() or "—"
    summary = {
        "total_spend": total_spend,
        "total_data": total_data,
        "cost_per_data": cost_per_data,
        "ads_percent": ads_percent,
    }
    advice_lines = build_advice_lines(summary, products)

    display_name = html.escape(user.get("display_name", username))
    timestamp = html.escape(now.strftime("%H:%M %d/%m/%Y"))
    product_lines = []
    for item in products[:TELEGRAM_REPORT_MAX_PRODUCTS]:
        product_lines.append(
            f"• <b>{html.escape(item['name'])}</b>: {item['spend']:,} VND | {item['data']:,} data | {item['cost_per_data']:,} VND/data"
        )

    if not product_lines:
        product_lines.append("• Chưa có dữ liệu sản phẩm cho hôm nay.")

    if len(products) > TELEGRAM_REPORT_MAX_PRODUCTS:
        product_lines.append(f"• ... và còn {len(products) - TELEGRAM_REPORT_MAX_PRODUCTS} sản phẩm khác.")

    advice_text = "\n".join(f"• {line}" for line in advice_lines)
    profitability_metrics = result.get("profitability_metrics") or {}
    completion_pct = profitability_metrics.get("completion_percent", {}).get("total", 0.0)
    gross_profit = profitability_metrics.get("gross_profit", {}).get("total", 0.0)
    gross_pct = profitability_metrics.get("gross_profit_percent", {}).get("total", 0.0)

    profit_lines = []
    if completion_pct:
        profit_lines.append(f"• % Hoàn dự tính: <b>{completion_pct:,.2f}%</b>")
    if gross_profit:
        profit_lines.append(f"• LN gộp (tháng): <b>{gross_profit:,.0f} VND</b>")
    if gross_pct:
        profit_lines.append(f"• %LN gộp: <b>{gross_pct:,.2f}%</b>")
    profit_section = ("\n<b>Lợi nhuận (tổng tháng)</b>\n" + "\n".join(profit_lines)) if profit_lines else ""

    message = (
        "<b>📊 Dạ thưa kính bẩm đại ca, em gửi báo cáo Ads realtime</b>\n"
        f"👤 {display_name}\n"
        f"🕒 {timestamp}\n\n"
        "<b>Báo cáo tổng quan hôm nay</b>\n"
        f"• Chi tiêu: <b>{total_spend:,} VND</b>\n"
        f"• Data: <b>{total_data:,}</b>\n"
        f"• Chi phí/data: <b>{cost_per_data:,} VND</b>\n"
        f"• % Ads: <b>{html.escape(ads_percent)}</b>"
        f"{profit_section}\n\n"
        "<b>Báo cáo theo sản phẩm</b>\n"
        f"{chr(10).join(product_lines)}\n\n"
        "<b>Cảnh báo / lời khuyên tạm thời</b>\n"
        f"{advice_text}\n\n"
        "<i>Dạ đại ca, em sẽ tiếp tục cập nhật theo lịch tự động.</i>"
    )
    return True, message, "OK"


def get_management_scope_users(owner_username: str, owner_user: dict, users: dict) -> list[tuple[str, dict]]:
    role = str(owner_user.get("role", "") or "").strip()
    if role == "admin":
        return [
            (uname, udata)
            for uname, udata in users.items()
            if udata.get("role") == "employee" and (udata.get("sheet_url") or "").strip()
        ]

    if role == "lead":
        team = str(owner_user.get("team", "") or "").strip()
        return [
            (uname, udata)
            for uname, udata in users.items()
            if udata.get("role") == "employee"
            and udata.get("team") == team
            and (udata.get("sheet_url") or "").strip()
        ]

    return []


def build_management_report_message(username: str, user: dict, now: datetime, users: dict) -> tuple[bool, str, str]:
    def build_product_top5_text(metrics: dict) -> str:
        product_payload = metrics.get("product_realtime") or {}
        top_items = product_payload.get("top5") or []
        if not top_items:
            return ""

        lines = []
        for idx, item in enumerate(top_items[:5], start=1):
            lines.append(
                f"• Top {idx}: <b>{html.escape(str(item.get('name_vn') or '—'))}</b> | "
                f"Data ra: <b>{round(float(item.get('data_out') or 0)):,}</b> | "
                f"Doanh số: <b>{round(float(item.get('revenue') or 0)):,} VND</b> | "
                f"Chi phí ads: <b>{round(float(item.get('spend') or 0)):,} VND</b>"
            )

        return "<b>Top 5 sản phẩm realtime hôm nay</b>\n" + "\n".join(lines)

    scope_users = get_management_scope_users(username, user, users)
    if not scope_users:
        performance_sheet_url = (user.get("performance_sheet_url") or "").strip()
        if not performance_sheet_url:
            return False, "", "Không có nhân viên nào có sheet để tổng hợp báo cáo."

        perf = fetch_performance_summary(performance_sheet_url)
        if not perf.get("success"):
            return False, "", perf.get("error", "Không đọc được bảng hiệu suất tổng.")

        metrics = perf.get("metrics") or {}
        revenue = metrics.get("revenue") or {}
        total_results = metrics.get("total_results") or {}
        total_spend = metrics.get("total_spend") or {}
        cpr = metrics.get("cost_per_result") or {}
        ads = metrics.get("ads_percent") or {}
        aov = metrics.get("avg_order_value") or {}
        completion = metrics.get("completion_percent") or {}
        gross = metrics.get("gross_profit") or {}
        product_top_text = build_product_top5_text(metrics)
        product_top_block = f"{product_top_text}\n\n" if product_top_text else ""

        display_name = html.escape(user.get("display_name", username))
        timestamp = html.escape(now.strftime("%H:%M %d/%m/%Y"))
        message = (
            "<b>📊 Dạ thưa kính bẩm đại ca, em gửi báo cáo tổng hợp Ads realtime</b>\n"
            f"👤 {display_name}\n"
            f"🕒 {timestamp}\n\n"
            "<b>Tổng quan KPI (nguồn: sheet hiệu suất)</b>\n"
            f"• Doanh số: <b>{round(float(revenue.get('month') or 0)):,} VND</b>\n"
            f"• Tổng kết quả: <b>{round(float(total_results.get('month') or 0)):,}</b>\n"
            f"• Chi phí: <b>{round(float(total_spend.get('month') or 0)):,} VND</b>\n"
            f"• Chi phí / kết quả: <b>{round(float(cpr.get('month') or 0)):,} VND</b>\n"
            f"• % Ads: <b>{round(float(ads.get('month') or 0), 2):,.2f}%</b>\n"
            f"• Giá trị TB đơn: <b>{round(float(aov.get('month') or 0)):,} VND</b>\n"
            f"• % Hoàn: <b>{round(float(completion.get('total') or 0), 2):,.2f}%</b>\n"
            f"• Lợi nhuận gộp: <b>{round(float(gross.get('total') or 0)):,} VND</b>\n\n"
            "<b>Realtime hôm nay</b>\n"
            f"• Doanh số hôm nay: <b>{round(float(revenue.get('day') or 0)):,} VND</b>\n"
            f"• Kết quả hôm nay: <b>{round(float(total_results.get('day') or 0)):,}</b>\n"
            f"• Chi phí hôm nay: <b>{round(float(total_spend.get('day') or 0)):,} VND</b>\n"
            f"• Chi phí / kết quả hôm nay: <b>{round(float(cpr.get('day') or 0)):,} VND</b>\n"
            f"• % Ads hôm nay: <b>{round(float(ads.get('day') or 0), 2):,.2f}%</b>\n"
            f"• Giá trị TB đơn hôm nay: <b>{round(float(aov.get('day') or 0)):,} VND</b>\n"
            f"• % Hoàn hôm nay: <b>{round(float(completion.get('day') or 0), 2):,.2f}%</b>\n"
            f"• Lợi nhuận gộp hôm nay: <b>{round(float(gross.get('day') or 0)):,} VND</b>\n\n"
            f"{product_top_block}"
            "<i>Dạ đại ca, em sẽ tiếp tục gửi đều theo lịch tự động.</i>"
        )
        return True, message, "OK"

    employee_summaries = []
    failed_users = []
    total_spend = 0
    total_data = 0

    for employee_username, employee_user in scope_users:
        sheet_url = (employee_user.get("sheet_url") or "").strip()
        sheet_id = extract_sheet_id(sheet_url)
        if not sheet_id:
            failed_users.append(employee_user.get("display_name", employee_username))
            continue

        result = fetch_chi_phi_ads_data(sheet_id)
        if not result.get("success"):
            failed_users.append(employee_user.get("display_name", employee_username))
            continue

        today = now.date()
        rows = [row for row in result.get("data", []) if parse_row_date(row.get("Ngày", "")) == today]
        spend = round(sum(parse_spend(row.get("Số tiền chi tiêu - VND", "")) for row in rows))
        data_count = sum(parse_int(row.get("Số Data", "")) for row in rows)
        total_spend += spend
        total_data += data_count
        employee_summaries.append({
            "name": employee_user.get("display_name", employee_username),
            "spend": spend,
            "data": data_count,
            "cost_per_data": round(spend / data_count) if data_count > 0 else 0,
        })

    employee_summaries.sort(key=lambda item: (item["spend"], item["data"]), reverse=True)
    summary_lines = []
    for item in employee_summaries[:10]:
        summary_lines.append(
            f"• <b>{html.escape(item['name'])}</b>: {item['spend']:,} VND | {item['data']:,} data | {item['cost_per_data']:,} VND/data"
        )

    if not summary_lines:
        summary_lines.append("• Hôm nay chưa đọc được dữ liệu nào từ các sheet nhân viên.")

    if len(employee_summaries) > 10:
        summary_lines.append(f"• ... và còn {len(employee_summaries) - 10} nhân viên khác.")

    if failed_users:
        failed_preview = ", ".join(html.escape(name) for name in failed_users[:5])
        summary_lines.append(f"• Không đọc được {len(failed_users)} sheet: {failed_preview}")

    product_top_block = ""
    performance_sheet_url = (user.get("performance_sheet_url") or "").strip()
    if performance_sheet_url:
        try:
            perf = fetch_performance_summary(performance_sheet_url)
            if perf.get("success"):
                product_top_text = build_product_top5_text(perf.get("metrics") or {})
                if product_top_text:
                    product_top_block = f"\n\n{product_top_text}"
        except Exception:
            product_top_block = ""

    display_name = html.escape(user.get("display_name", username))
    timestamp = html.escape(now.strftime("%H:%M %d/%m/%Y"))
    total_cost_per_data = round(total_spend / total_data) if total_data > 0 else 0
    message = (
        "<b>📊 Dạ thưa kính bẩm đại ca, em gửi báo cáo tổng hợp Ads realtime</b>\n"
        f"👤 {display_name}\n"
        f"🕒 {timestamp}\n\n"
        "<b>Tổng quan quản trị hôm nay</b>\n"
        f"• Nhân viên có sheet: <b>{len(scope_users):,}</b>\n"
        f"• Sheet đọc được: <b>{len(employee_summaries):,}</b>\n"
        f"• Tổng chi tiêu: <b>{total_spend:,} VND</b>\n"
        f"• Tổng data: <b>{total_data:,}</b>\n"
        f"• Chi phí/data gộp: <b>{total_cost_per_data:,} VND</b>\n\n"
        "<b>Tổng hợp theo nhân viên</b>\n"
        f"{chr(10).join(summary_lines)}"
        f"{product_top_block}\n\n"
        "<i>Dạ đại ca, em sẽ tiếp tục gửi đều theo lịch tự động.</i>"
    )
    return True, message, "OK"


def run_telegram_report_job(*, force: bool = False, dry_run: bool = False, usernames: Optional[list] = None) -> dict:
    now = get_notification_now()
    slot = get_notification_slot(now)
    if not force and not is_notification_window(now):
        return {"success": True, "slot": slot, "skipped": True, "reason": "outside_window", "results": []}

    state = load_telegram_report_state()
    last_slot = state.get("last_slot", "")
    if not force and last_slot == slot:
        return {"success": True, "slot": slot, "skipped": True, "reason": "already_sent", "results": []}

    users = load_users_config()
    selected = set(usernames or [])
    results = []
    sent_count = 0

    for username, user in users.items():
        if not is_telegram_report_role(user):
            continue
        if selected and username not in selected:
            continue

        chat_id = (user.get("telegram_chat_id") or "").strip()
        if not chat_id:
            results.append({"username": username, "status": "skipped", "reason": "missing_chat"})
            continue

        role = str(user.get("role", "") or "").strip()
        if role == "employee":
            sheet_url = (user.get("sheet_url") or "").strip()
            if not sheet_url:
                results.append({"username": username, "status": "skipped", "reason": "missing_sheet"})
                continue
            ok, message, info = build_employee_report_message(username, user, now)
        else:
            ok, message, info = build_management_report_message(username, user, now, users)
        if not ok:
            results.append({"username": username, "status": "failed", "reason": info})
            continue


        if dry_run:
            print(f"\n===== {username} =====\n{message}\n")
            results.append({"username": username, "status": "dry_run"})
            sent_count += 1
            continue

        personal_bot_token = normalize_telegram_bot_token(str(user.get("telegram_bot_token", "")))
        send_ok, send_info = send_telegram_message(chat_id, message, bot_token=personal_bot_token)
        results.append({
            "username": username,
            "status": "sent" if send_ok else "failed",
            "reason": send_info,
        })
        if send_ok:
            sent_count += 1

    if sent_count > 0 and not dry_run:
        state["last_slot"] = slot
        state["last_run_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
        save_telegram_report_state(state)

    return {"success": True, "slot": slot, "skipped": False, "results": results, "sent_count": sent_count}


def trigger_telegram_report_job_command(force: bool = False) -> int:
    if not TELEGRAM_REPORT_ENDPOINT:
        print("Thieu TELEGRAM_REPORT_ENDPOINT.")
        return 1
    if not INTERNAL_CRON_SECRET:
        print("Thieu INTERNAL_CRON_SECRET.")
        return 1

    payload = json.dumps({"force": bool(force)}).encode("utf-8")
    req = urllib_request.Request(
        url=TELEGRAM_REPORT_ENDPOINT,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {INTERNAL_CRON_SECRET}",
        },
        method="POST",
    )

    try:
        with urllib_request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            print(body)
            return 0
    except Exception as exc:
        print(f"Trigger job that bai: {exc}")
        return 1


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


def get_service_account_client_email() -> str:
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if service_account_json:
        try:
            service_account_info = json.loads(service_account_json)
            return str(service_account_info.get("client_email", "")).strip()
        except Exception:
            return ""

    if SERVICE_ACCOUNT_PATH.exists():
        try:
            with SERVICE_ACCOUNT_PATH.open("r", encoding="utf-8") as f:
                service_account_info = json.load(f)
            return str(service_account_info.get("client_email", "")).strip()
        except Exception:
            return ""

    return ""


def build_sheet_access_links(sheet_id: str) -> dict:
    clean_sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    share_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit?usp=sharing"
    request_access_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit?usp=drivesdk"
    return {
        "clean_url": clean_sheet_url,
        "share_url": share_url,
        "request_access_url": request_access_url,
    }


def build_sheet_access_help(service_email: str, sheet_id: str = "") -> tuple[str, list, dict]:
    share_target = service_email or "email service account của hệ thống"
    links = build_sheet_access_links(sheet_id) if sheet_id else {}

    sheet_hint = ""
    if links.get("share_url"):
        sheet_hint = f" Mở nhanh: {links['share_url']}"

    help_text = (
        f"Mở Google Sheet > Chia sẻ > thêm {share_target} (quyền Người chỉnh sửa hoặc Người xem), "
        f"sau đó bấm tải lại.{sheet_hint}"
    )
    steps = [
        "Mở Google Sheet bạn vừa nhập.",
        "Bấm nút Chia sẻ ở góc phải trên (hoặc dùng link Mở nhanh nếu có).",
        f"Thêm {share_target} với quyền Người chỉnh sửa hoặc Người xem.",
        "Bấm Xong, quay lại dashboard và tải lại link sheet.",
    ]

    if links.get("request_access_url"):
        steps.append("Nếu bạn không phải chủ sheet: bấm link Yêu cầu quyền truy cập để gửi yêu cầu cho chủ sở hữu.")

    return help_text, steps, links


def inspect_sheet_access(sheet_url: str) -> dict:
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return {
            "success": False,
            "error": "URL Google Sheet không hợp lệ.",
            "help": "Vui lòng dùng link dạng https://docs.google.com/spreadsheets/d/...",
            "help_steps": [],
            "service_account_email": get_service_account_client_email(),
        }

    access_links = build_sheet_access_links(sheet_id)
    clean_url = access_links.get("clean_url", "")
    service_email = get_service_account_client_email()

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        sheet_name = spreadsheet.title or sheet_id
        month_key = detect_month_key_from_text(sheet_name)
        return {
            "success": True,
            "sheet_name": sheet_name,
            "month_key": month_key,
            "clean_url": clean_url,
            "sheet_id": sheet_id,
            "service_account_email": service_email,
            "auto_connected": True,
            "share_url": access_links.get("share_url", ""),
            "request_access_url": access_links.get("request_access_url", ""),
        }
    except FileNotFoundError:
        return {
            "success": False,
            "error": "Hệ thống chưa được cấu hình service account Google Sheets.",
            "help": "Liên hệ quản trị viên để cấu hình GOOGLE_SERVICE_ACCOUNT_JSON hoặc file service_account.json.",
            "help_steps": [],
            "service_account_email": service_email,
        }
    except gspread.exceptions.SpreadsheetNotFound:
        help_text, steps, links = build_sheet_access_help(service_email, sheet_id)
        return {
            "success": False,
            "error": "Hệ thống chưa có quyền truy cập sheet này.",
            "help": help_text,
            "help_steps": steps,
            "service_account_email": service_email,
            "clean_url": clean_url,
            "sheet_id": sheet_id,
            "share_url": links.get("share_url", ""),
            "request_access_url": links.get("request_access_url", ""),
            "can_auto_open_sheet": True,
        }
    except Exception as e:
        raw_error = str(e)
        lower_error = raw_error.lower()
        if "permission" in lower_error or "forbidden" in lower_error or "<response [403]>" in lower_error:
            help_text, steps, links = build_sheet_access_help(service_email, sheet_id)
            return {
                "success": False,
                "error": "Sheet chưa cấp quyền cho hệ thống.",
                "help": help_text,
                "help_steps": steps,
                "service_account_email": service_email,
                "clean_url": clean_url,
                "sheet_id": sheet_id,
                "share_url": links.get("share_url", ""),
                "request_access_url": links.get("request_access_url", ""),
                "can_auto_open_sheet": True,
            }

        return {
            "success": False,
            "error": raw_error,
            "help": "Vui lòng kiểm tra lại link sheet và thử lại.",
            "help_steps": [],
            "service_account_email": service_email,
            "clean_url": clean_url,
            "sheet_id": sheet_id,
            "share_url": access_links.get("share_url", ""),
            "request_access_url": access_links.get("request_access_url", ""),
        }


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


def save_monthly_sheet_record(
    username: str,
    sheet_url: str,
    sheet_name: str,
    month_key: str,
    performance_sheet_url: str = "",
    performance_sheet_name: str = "",
) -> None:
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
    if performance_sheet_url:
        record["performance_sheet_url"] = performance_sheet_url
        if performance_sheet_name:
            record["performance_sheet_name"] = performance_sheet_name
    elif existing_idx >= 0 and isinstance(entries[existing_idx], dict):
        # Keep the previously saved performance URL when user only updates ads sheet.
        prev_perf_url = (entries[existing_idx].get("performance_sheet_url") or "").strip()
        if prev_perf_url:
            record["performance_sheet_url"] = prev_perf_url
            prev_perf_name = (entries[existing_idx].get("performance_sheet_name") or "").strip()
            if prev_perf_name:
                record["performance_sheet_name"] = prev_perf_name
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
                perf_url = (latest.get("performance_sheet_url") or "").strip()
                perf_name = (latest.get("performance_sheet_name") or "").strip()
                # Backward-compat: old records may have URL but no title.
                if perf_url and not perf_name:
                    guessed_name, _, _ = get_sheet_name_and_month(perf_url)
                    perf_name = guessed_name or "Bảng hiệu suất"
                result.append({
                    "month_key": month_key,
                    "month_label": month_label(month_key),
                    "sheet_name": latest.get("sheet_name", ""),
                    "sheet_url": latest.get("sheet_url", ""),
                    "performance_sheet_url": perf_url,
                    "performance_sheet_name": perf_name,
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
            "performance_sheet_url": "",
            "performance_sheet_name": "",
            "folder_url": url_for("view_month_folder", month_key=mk),
        })

    return result


def set_session_user(
    username: str,
    user: dict,
    *,
    elevated: bool = False,
    session_role: Optional[str] = None,
) -> None:
    session["logged_in"] = True
    session["username"] = username
    actual_role = str(user.get("role", "employee") or "employee")
    effective_role = str(session_role or actual_role or "employee")
    session["role"] = effective_role
    session["account_role"] = actual_role
    session["team"] = user.get("team", "")
    session["display_name"] = user.get("display_name", username)
    session["sheet_url"] = user.get("sheet_url", "")
    session["performance_sheet_url"] = user.get("performance_sheet_url", "")
    session["is_elevated"] = elevated
    session["last_activity"] = datetime.now().timestamp()
    if actual_role == "employee" and not elevated:
        session["base_employee"] = {
            "username": username,
            "display_name": user.get("display_name", username),
            "team": user.get("team", ""),
            "sheet_url": user.get("sheet_url", ""),
            "performance_sheet_url": user.get("performance_sheet_url", ""),
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

def is_session_timeout():
    """Check if the current session has exceeded the timeout duration."""
    if session.get("logged_in") is not True:
        return False

    last_activity = session.get("last_activity", 0)
    if not last_activity:
        return False

    current_time = datetime.now().timestamp()
    elapsed = current_time - last_activity
    return elapsed > SESSION_TIMEOUT_SECONDS


def is_logged_in():
    if session.get("logged_in") is not True:
        return False

    # Check for session timeout
    if is_session_timeout():
        session.clear()
        return False

    return True


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            # Session might have timed out
            return redirect(url_for("login", next=request.path))

        username = session.get("username", "")
        role = session.get("role", "employee")
        account_role = session.get("account_role", role)
        if (
            role == "employee"
            and account_role == "employee"
            and request.endpoint != "employee_telegram_connect"
            and user_requires_telegram_setup(username)
        ):
            return redirect(url_for("employee_telegram_connect", next=request.path))
        return view_func(*args, **kwargs)
    return wrapper


def api_login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            # Session might have timed out or user not logged in
            return jsonify({"success": False, "error": "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."}), 401

        username = session.get("username", "")
        role = session.get("role", "employee")
        account_role = session.get("account_role", role)
        if role == "employee" and account_role == "employee" and user_requires_telegram_setup(username):
            return jsonify({
                "success": False,
                "error": "Vui lòng kết nối Telegram và gửi test thành công trước khi sử dụng hệ thống.",
                "setup_url": url_for("employee_telegram_connect"),
            }), 428
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
    """Extract sheet ID from common Google Sheets URL formats or raw ID."""
    raw = str(url or "")
    if not raw:
        return None

    # Remove angle brackets/quotes from copied text and normalize hidden chars.
    cleaned = raw.strip().strip("<>'\"")
    cleaned = urllib_parse.unquote(cleaned)
    cleaned = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", cleaned)
    cleaned = re.sub(r"\s+", "", cleaned)

    # Raw ID only (user may paste just the sheet key).
    if re.fullmatch(r"[a-zA-Z0-9_-]{20,}", cleaned):
        return cleaned

    patterns = [
        r"/spreadsheets/(?:u/\d+/)?d/([a-zA-Z0-9_-]{20,})",
        r"[?&]id=([a-zA-Z0-9_-]{20,})",
    ]
    for pattern in patterns:
        match = re.search(pattern, cleaned)
        if match:
            return match.group(1)
    return None


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


def normalize_account_id(raw_id: str) -> str:
    value = (raw_id or "").strip()
    if value.startswith("act_"):
        value = value[4:]
    return re.sub(r"[^\d]", "", value)


def resolve_settings_worksheet(spreadsheet):
    preferred_titles = [
        "Cài đặt",
        "Cai dat",
        "Thiết lập",
        "Thiet lap",
        "Settings",
        "SETTING",
    ]
    for title in preferred_titles:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue

    try:
        for ws in spreadsheet.worksheets():
            norm = normalize_sheet_tab_name(ws.title)
            if "setting" in norm or "caidat" in norm or "thietlap" in norm:
                return ws
    except Exception:
        pass

    raise gspread.exceptions.WorksheetNotFound("Cài đặt")


def extract_accounts_from_settings_rows(rows: list) -> list:
    entries = []
    seen = set()

    for row in rows:
        candidate = row[6].strip() if len(row) > 6 else ""
        if not candidate:
            continue

        account_id = ""
        match_parenthesis = re.search(r"\((act_?\d+)\)", candidate, re.IGNORECASE)
        if match_parenthesis:
            account_id = normalize_account_id(match_parenthesis.group(1))
        if not account_id:
            match_act = re.search(r"\bact_(\d+)\b", candidate, re.IGNORECASE)
            if match_act:
                account_id = normalize_account_id(match_act.group(1))
        if not account_id:
            match_digits = re.search(r"\b(\d{8,})\b", candidate)
            if match_digits:
                account_id = normalize_account_id(match_digits.group(1))

        if not account_id or account_id in seen:
            continue

        name = candidate
        if "(" in name:
            name = name.split("(", 1)[0].strip()
        if not name:
            name = f"act_{account_id}"

        seen.add(account_id)
        entries.append({
            "account_id": account_id,
            "account_name": name,
        })

    return entries


def load_meta_access_token() -> str:
    token_from_env = (os.getenv("META_ACCESS_TOKEN", "") or "").strip()
    if token_from_env:
        return token_from_env

    if not META_ACCESS_TOKEN_PATH.exists():
        return ""

    try:
        with META_ACCESS_TOKEN_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            for key in ("access_token", "token", "meta_access_token"):
                val = str(payload.get(key, "") or "").strip()
                if val:
                    return val
        if isinstance(payload, str):
            return payload.strip()
    except Exception:
        return ""

    return ""


def load_meta_token_vault() -> dict:
    """Load keyed Meta tokens used for auto-routing by token_key.

    Supported JSON formats:
    - {"default": "...", "tokens": {"team_3": "EA..."}}
    - {"team_3": "EA...", "team_1": "EA..."}
    """
    env_payload = (os.getenv("META_TOKEN_VAULT", "") or "").strip()
    if env_payload:
        try:
            parsed = json.loads(env_payload)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    if not META_TOKEN_VAULT_PATH.exists():
        return {}

    try:
        with META_TOKEN_VAULT_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _token_from_vault(vault: dict, token_key: str) -> str:
    key = str(token_key or "").strip()
    if not key:
        return ""

    nested = vault.get("tokens") if isinstance(vault, dict) else None
    if isinstance(nested, dict):
        value = str(nested.get(key, "") or "").strip()
        if value:
            return value

    direct = str(vault.get(key, "") or "").strip() if isinstance(vault, dict) else ""
    return direct


def resolve_meta_access_token(token_key: str = "") -> str:
    """Resolve token by priority: keyed vault -> vault default -> legacy token."""
    vault = load_meta_token_vault()
    token = _token_from_vault(vault, token_key)
    if token:
        return token

    default_from_vault = str(vault.get("default", "") or "").strip() if isinstance(vault, dict) else ""
    if default_from_vault:
        return default_from_vault

    return load_meta_access_token()


def resolve_meta_account_map_worksheet(spreadsheet):
    preferred = [
        "Meta_Account_Map",
        "Meta Account Map",
        "META_ACCOUNT_MAP",
        "Meta mapping",
        "Meta_Map",
        "Map tài khoản Meta",
    ]
    for title in preferred:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue

    try:
        for ws in spreadsheet.worksheets():
            norm = normalize_sheet_tab_name(ws.title)
            if "meta" in norm and ("map" in norm or "mapping" in norm):
                return ws
    except Exception:
        pass

    return None


def _parse_bool_like(value: str, default: bool = True) -> bool:
    text = normalize_sheet_tab_name(value)
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on", "active", "kichhoat"}:
        return True
    if text in {"0", "false", "no", "n", "off", "inactive", "tat", "disabled"}:
        return False
    return default


def _detect_meta_map_headers(row: list) -> dict:
    headers = {}
    for idx, cell in enumerate(row):
        h = normalize_sheet_tab_name(cell)
        if not h:
            continue
        if "owner" in h or h in {"username", "nguoidung", "nhanvien", "taikhoan"}:
            headers.setdefault("owner_username", idx)
        if ("account" in h and "id" in h) or h in {"metaid", "adaccountid", "actid"}:
            headers.setdefault("meta_account_id", idx)
        if ("label" in h and "account" in h) or "tenhienthi" in h or "tentaikhoan" in h:
            headers.setdefault("account_label", idx)
        if "tokenkey" in h or ("token" in h and "key" in h):
            headers.setdefault("token_key", idx)
        if "active" in h or "isactive" in h or "trangthai" in h:
            headers.setdefault("is_active", idx)
        if "priority" in h or "uutien" in h:
            headers.setdefault("priority", idx)
    return headers


def _safe_cell(row: list, idx: Optional[int]) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return str(row[idx] or "").strip()


def load_accounts_from_meta_map(spreadsheet, owner_username: str = "") -> list:
    ws = resolve_meta_account_map_worksheet(spreadsheet)
    if ws is None:
        return []

    rows = ws.get_all_values()
    if not rows:
        return []

    header_idx = None
    header_map = {}
    for i, row in enumerate(rows[:20]):
        detected = _detect_meta_map_headers(row)
        if "meta_account_id" in detected:
            header_idx = i
            header_map = detected
            break

    if header_idx is None:
        return []

    owner_norm = normalize_username(owner_username)
    entries = []
    dedupe = {}
    for row in rows[header_idx + 1 :]:
        raw_account = _safe_cell(row, header_map.get("meta_account_id"))
        account_id = normalize_account_id(raw_account)
        if not account_id:
            continue

        row_owner = _safe_cell(row, header_map.get("owner_username"))
        if owner_norm and row_owner and normalize_username(row_owner) != owner_norm:
            continue

        is_active = _parse_bool_like(_safe_cell(row, header_map.get("is_active")), default=True)
        if not is_active:
            continue

        label = _safe_cell(row, header_map.get("account_label")) or f"act_{account_id}"
        token_key = _safe_cell(row, header_map.get("token_key"))
        priority_raw = parse_number_like(_safe_cell(row, header_map.get("priority")))
        priority = int(priority_raw) if priority_raw is not None else 999

        existing = dedupe.get(account_id)
        candidate = {
            "account_id": account_id,
            "account_name": label,
            "token_key": token_key,
            "priority": priority,
            "source": "meta_map",
        }
        if existing is None or candidate["priority"] < existing["priority"]:
            dedupe[account_id] = candidate

    entries = list(dedupe.values())
    entries.sort(key=lambda x: (int(x.get("priority", 999)), str(x.get("account_name", ""))))
    return entries


def resolve_accounts_for_meta_sync(spreadsheet, owner_username: str = "") -> list:
    mapped = load_accounts_from_meta_map(spreadsheet, owner_username=owner_username)
    if mapped:
        return mapped

    settings_ws = resolve_settings_worksheet(spreadsheet)
    fallback = extract_accounts_from_settings_rows(settings_ws.get_all_values())
    for item in fallback:
        item["token_key"] = ""
        item["priority"] = 999
        item["source"] = "settings"
    return fallback


def fetch_meta_account_status(account_id: str, access_token: str) -> dict:
    if not access_token:
        return {
            "status": "not_connected",
            "status_label": "Chưa kết nối API",
            "spend_today": 0.0,
            "hint": "Hệ thống chưa có Meta access token. Quản trị chỉ cần cập nhật token một lần để web tự chạy.",
        }

    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{account_id}/insights"
    query = urllib_parse.urlencode(
        {
            "fields": "spend",
            "date_preset": "today",
            "limit": 1,
            "access_token": access_token,
        }
    )
    url = f"{endpoint}?{query}"

    try:
        with urllib_request.urlopen(url, timeout=20) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        data = payload.get("data", []) if isinstance(payload, dict) else []
        spend = 0.0
        if data and isinstance(data, list):
            first = data[0] if isinstance(data[0], dict) else {}
            spend = float(first.get("spend") or 0)

        if spend > 0:
            return {
                "status": "has_spend",
                "status_label": "Có chi tiêu",
                "spend_today": spend,
                "hint": "Đã kết nối API và ghi nhận chi tiêu hôm nay.",
            }

        return {
            "status": "no_spend",
            "status_label": "Đã kết nối - chưa chi tiêu",
            "spend_today": 0.0,
            "hint": "API hoạt động bình thường, hôm nay chưa phát sinh chi tiêu.",
        }
    except urllib_error.HTTPError as e:
        error_message = ""
        try:
            body = e.read().decode("utf-8")
            payload = json.loads(body)
            error_message = str((payload.get("error") or {}).get("message") or "")
        except Exception:
            error_message = ""

        hint = "Web chưa thể tự kết nối tài khoản này. Nhân viên chỉ cần vào Business Manager cấp quyền ads_read cho token hiện tại."
        if "access token" in error_message.lower() or "oauth" in error_message.lower():
            hint = "Token Meta đã hết hạn hoặc thiếu quyền. Quản trị cập nhật lại token để web tự chạy lại toàn bộ."

        return {
            "status": "not_connected",
            "status_label": "Chưa kết nối API",
            "spend_today": 0.0,
            "hint": hint,
            "error": error_message or f"HTTP {e.code}",
        }
    except Exception as e:
        return {
            "status": "not_connected",
            "status_label": "Chưa kết nối API",
            "spend_today": 0.0,
            "hint": "Web chưa thể tự kết nối tài khoản này. Vui lòng kiểm tra lại quyền truy cập tài khoản quảng cáo.",
            "error": str(e),
        }


def get_sheet_account_statuses(sheet_id: str) -> dict:
    client = get_gspread_client()
    spreadsheet = client.open_by_key(sheet_id)
    owner_username = str(session.get("username", "") or "")
    accounts = resolve_accounts_for_meta_sync(spreadsheet, owner_username=owner_username)

    default_token = resolve_meta_access_token("")
    results = []
    summary = {
        "total": 0,
        "has_spend": 0,
        "no_spend": 0,
        "not_connected": 0,
    }

    status_weight = {"has_spend": 0, "no_spend": 1, "not_connected": 2}
    for entry in accounts:
        account_token = resolve_meta_access_token(entry.get("token_key", ""))
        status_data = fetch_meta_account_status(entry["account_id"], account_token)
        status = status_data.get("status", "not_connected")
        summary["total"] += 1
        summary[status] = summary.get(status, 0) + 1
        results.append(
            {
                "account_id": entry["account_id"],
                "account_name": entry["account_name"],
                "status": status,
                "status_label": status_data.get("status_label", "Chưa rõ"),
                "spend_today": float(status_data.get("spend_today") or 0),
                "hint": status_data.get("hint", ""),
                "error": status_data.get("error", ""),
                "token_key": entry.get("token_key", ""),
                "source": entry.get("source", "settings"),
            }
        )

    results.sort(key=lambda item: (status_weight.get(item.get("status", ""), 9), -float(item.get("spend_today", 0))))
    return {
        "success": True,
        "summary": summary,
        "accounts": results,
        "has_token": bool(default_token),
    }


def extract_product_name_from_campaign(campaign_name: str) -> str:
    match = re.search(r"BID\s*1_(.+?)(?:_|$)", campaign_name or "", re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return (campaign_name or "").strip() or "Không rõ sản phẩm"


def sum_result_actions(actions: list) -> int:
    total = 0
    for action in actions or []:
        action_type = str(action.get("action_type", ""))
        if action_type == "offsite_conversion.fb_pixel_complete_registration":
            try:
                total += int(float(action.get("value", 0)))
            except (TypeError, ValueError):
                continue
    return total


def format_date_vn(date_obj: datetime) -> str:
    return date_obj.strftime("%d/%m/%Y")


def extract_account_id_from_label(label: str) -> str:
    match = re.search(r"\((\d{6,})\)", label or "")
    return normalize_account_id(match.group(1)) if match else ""


def resolve_account_name_for_sheet(all_values: list, account_id: str, account_name_raw: str) -> str:
    normalized_id = normalize_account_id(account_id)
    for i, row in enumerate(all_values):
        if i < SHEET_DATA_START_ROW - 1:
            continue
        cell_account = row[1].strip() if len(row) > 1 else ""
        if cell_account and normalized_id and f"({normalized_id})" in cell_account:
            return cell_account

    cleaned_name = (account_name_raw or "").strip() or f"act_{normalized_id}"
    if normalized_id and f"({normalized_id})" not in cleaned_name:
        return f"{cleaned_name} ({normalized_id})"
    return cleaned_name


def has_core_data_in_ads_row(row: list) -> bool:
    cell_date = row[0].strip() if len(row) > 0 else ""
    cell_account = row[1].strip() if len(row) > 1 else ""
    cell_product = row[3].strip() if len(row) > 3 else ""
    return bool(cell_date and (cell_account or cell_product))


def detect_logical_last_data_row(all_values: list) -> int:
    last_data_row = SHEET_DATA_START_ROW - 1
    seen_data = False
    empty_streak = 0

    for i in range(SHEET_DATA_START_ROW - 1, len(all_values)):
        row = all_values[i]
        if has_core_data_in_ads_row(row):
            seen_data = True
            empty_streak = 0
            last_data_row = i + 1
            continue

        if seen_data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_STREAK_TO_STOP_SCAN:
                break

    return last_data_row


def aggregate_sync_rows(rows_to_write: list) -> list:
    grouped = {}
    for row in rows_to_write:
        account_id = normalize_account_id(str(row.get("account_id", "")))
        key = (row.get("date_vn", ""), account_id or row.get("account_name", ""), row.get("product_name", ""))
        if key not in grouped:
            grouped[key] = {
                "date_vn": row.get("date_vn", ""),
                "account_id": account_id,
                "account_name": row.get("account_name", ""),
                "product_name": row.get("product_name", ""),
                "data_count": int(row.get("data_count", 0) or 0),
                "spend": float(row.get("spend", 0) or 0),
            }
            continue

        grouped[key]["data_count"] += int(row.get("data_count", 0) or 0)
        grouped[key]["spend"] += float(row.get("spend", 0) or 0)

    return list(grouped.values())


def upsert_rows_to_ads_worksheet(worksheet, rows_to_write: list) -> int:
    prepared_rows = aggregate_sync_rows(rows_to_write)
    if not prepared_rows:
        return 0

    all_values = worksheet.get_all_values()
    logical_last_data_row = detect_logical_last_data_row(all_values)
    written = 0

    for row_data in prepared_rows:
        target_date = row_data.get("date_vn", "")
        account_id = normalize_account_id(str(row_data.get("account_id", "")))
        account_name = resolve_account_name_for_sheet(all_values, account_id, row_data.get("account_name", ""))
        product_name = (row_data.get("product_name", "") or "").strip() or "Không rõ sản phẩm"
        data_count = int(row_data.get("data_count", 0) or 0)
        spend = float(row_data.get("spend", 0) or 0)

        found_row_idx = None
        found_cell_account = ""
        for i, row in enumerate(all_values):
            if i < SHEET_DATA_START_ROW - 1:
                continue
            if i + 1 > logical_last_data_row:
                break

            cell_date = row[0].strip() if len(row) > 0 else ""
            cell_account = row[1].strip() if len(row) > 1 else ""
            cell_product = row[3].strip() if len(row) > 3 else ""

            same_account = cell_account == account_name
            if not same_account and account_id:
                cell_account_id = extract_account_id_from_label(cell_account)
                if cell_account_id == account_id:
                    same_account = True
                elif row_data.get("account_name") and cell_account.startswith(str(row_data.get("account_name"))):
                    same_account = True

            if cell_date == target_date and same_account and cell_product == product_name:
                found_row_idx = i + 1
                found_cell_account = cell_account
                break

        if found_row_idx:
            worksheet.update(f"A{found_row_idx}", [[target_date]], value_input_option="USER_ENTERED")
            if found_cell_account != account_name:
                worksheet.update(f"B{found_row_idx}", [[account_name]])
            worksheet.update(f"E{found_row_idx}:F{found_row_idx}", [[data_count, spend]])
        else:
            next_row = logical_last_data_row + 1
            if next_row > worksheet.row_count:
                worksheet.add_rows(next_row - worksheet.row_count)

            worksheet.update(
                f"A{next_row}:B{next_row}",
                [[target_date, account_name]],
                value_input_option="USER_ENTERED",
            )
            worksheet.update(f"D{next_row}:F{next_row}", [[product_name, data_count, spend]])

            while len(all_values) < next_row:
                all_values.append([])
            all_values[next_row - 1] = [target_date, account_name, "", product_name, str(data_count), str(spend)]
            logical_last_data_row = next_row

        written += 1

    return written


def fetch_meta_campaign_insights(account_id: str, access_token: str, date_preset: str = "today") -> list:
    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{normalize_account_id(account_id)}/insights"
    query = urllib_parse.urlencode(
        {
            "fields": "campaign_name,spend,actions",
            "level": "campaign",
            "date_preset": date_preset,
            "limit": 200,
            "access_token": access_token,
        }
    )
    url = f"{endpoint}?{query}"
    with urllib_request.urlopen(url, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return data if isinstance(data, list) else []


def sync_ads_sheet_from_meta(sheet_id: str, date_preset: str = "today", owner_username: str = "") -> dict:
    default_token = resolve_meta_access_token("")
    if not default_token:
        return {
            "attempted": True,
            "success": False,
            "written_rows": 0,
            "accounts_total": 0,
            "accounts_synced": 0,
            "hint": "Chưa có Meta access token, nên chưa thể tự ghi chi phí vào sheet.",
        }

    client = get_gspread_client()
    spreadsheet = client.open_by_key(sheet_id)
    ads_ws = resolve_ads_worksheet(spreadsheet)
    accounts = resolve_accounts_for_meta_sync(spreadsheet, owner_username=owner_username)

    now_dt = datetime.now() if date_preset == "today" else datetime.now() - timedelta(days=1)
    target_date_vn = format_date_vn(now_dt)
    rows_to_write = []
    sync_errors = []
    synced_accounts = 0

    for account in accounts:
        account_id = normalize_account_id(account.get("account_id", ""))
        account_name = (account.get("account_name") or f"act_{account_id}").strip()
        token_key = str(account.get("token_key", "") or "").strip()
        if not account_id:
            continue

        account_token = resolve_meta_access_token(token_key)
        if not account_token:
            sync_errors.append(f"act_{account_id}: thiếu token (token_key={token_key or 'default'})")
            continue

        try:
            campaigns = fetch_meta_campaign_insights(account_id, account_token, date_preset=date_preset)
        except urllib_error.HTTPError as e:
            sync_errors.append(f"act_{account_id}: HTTP {e.code}")
            continue
        except Exception as e:
            sync_errors.append(f"act_{account_id}: {str(e)}")
            continue

        if campaigns:
            synced_accounts += 1

        for campaign in campaigns:
            spend = float(campaign.get("spend") or 0)
            if spend <= 0:
                continue
            rows_to_write.append(
                {
                    "date_vn": target_date_vn,
                    "account_id": account_id,
                    "account_name": account_name,
                    "product_name": extract_product_name_from_campaign(campaign.get("campaign_name", "")),
                    "data_count": sum_result_actions(campaign.get("actions") or []),
                    "spend": spend,
                }
            )

    written_rows = upsert_rows_to_ads_worksheet(ads_ws, rows_to_write) if rows_to_write else 0
    return {
        "attempted": True,
        "success": True,
        "written_rows": written_rows,
        "accounts_total": len(accounts),
        "accounts_synced": synced_accounts,
        "accounts_source": "meta_map" if any(a.get("source") == "meta_map" for a in accounts) else "settings",
        "errors": sync_errors[:10],
        "hint": "" if not sync_errors else "Một số tài khoản chưa tự đồng bộ được, web đã bỏ qua và vẫn tải dữ liệu hiện có.",
    }


def run_ads_autofill_job(date_preset: str = "today") -> dict:
    registry = parse_sheet_registry()
    if not registry:
        return {
            "success": False,
            "mode": date_preset,
            "error": "Không có sheet nào trong danh sách sheet_urls.",
            "sheets_total": 0,
            "sheets_processed": 0,
            "total_written_rows": 0,
            "results": [],
        }

    unique_sheet_ids = set()
    results = []
    sheets_processed = 0
    total_written_rows = 0
    success_count = 0

    for item in registry:
        sheet_url = (item.get("url") or "").strip()
        sheet_name = (item.get("name") or "").strip() or "Sheet"
        sheet_id = extract_sheet_id(sheet_url)
        if not sheet_id or sheet_id in unique_sheet_ids:
            continue

        unique_sheet_ids.add(sheet_id)
        sheets_processed += 1
        try:
            sync_result = sync_ads_sheet_from_meta(sheet_id, date_preset=date_preset, owner_username="")
            written_rows = int(sync_result.get("written_rows", 0) or 0)
            total_written_rows += written_rows
            if sync_result.get("success", False):
                success_count += 1
            results.append(
                {
                    "sheet_name": sheet_name,
                    "sheet_id": sheet_id,
                    "success": bool(sync_result.get("success", False)),
                    "written_rows": written_rows,
                    "accounts_total": int(sync_result.get("accounts_total", 0) or 0),
                    "accounts_synced": int(sync_result.get("accounts_synced", 0) or 0),
                    "hint": sync_result.get("hint", ""),
                    "errors": sync_result.get("errors", [])[:5],
                }
            )
        except Exception as exc:
            results.append(
                {
                    "sheet_name": sheet_name,
                    "sheet_id": sheet_id,
                    "success": False,
                    "written_rows": 0,
                    "accounts_total": 0,
                    "accounts_synced": 0,
                    "hint": f"Lỗi xử lý sheet: {str(exc)}",
                    "errors": [str(exc)],
                }
            )

    return {
        "success": success_count > 0,
        "mode": date_preset,
        "sheets_total": len(unique_sheet_ids),
        "sheets_processed": sheets_processed,
        "sheets_success": success_count,
        "total_written_rows": total_written_rows,
        "results": results,
    }


def parse_number_like(value: str) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None

    compact = text.replace("\xa0", " ").replace("₫", "").replace("VND", "").replace("vnd", "")
    compact = compact.replace("%", "")
    compact = re.sub(r"[^0-9,\.\-]", "", compact)

    # Normalize decimal/thousand separators safely.
    has_dot = "." in compact
    has_comma = "," in compact
    if has_dot and has_comma:
        # Detect decimal separator by the last punctuation position.
        if compact.rfind(",") > compact.rfind("."):
            # VN style: 1.234.567,89
            compact = compact.replace(".", "").replace(",", ".")
        else:
            # US style: 1,234,567.89
            compact = compact.replace(",", "")
    elif has_comma and not has_dot:
        parts = compact.split(",")
        if len(parts) == 2 and len(parts[1]) <= 2:
            compact = parts[0] + "." + parts[1]
        else:
            compact = compact.replace(",", "")
    elif has_dot and not has_comma:
        parts = compact.split(".")
        if len(parts) == 2 and len(parts[1]) <= 2:
            compact = parts[0] + "." + parts[1]
        else:
            compact = compact.replace(".", "")

    if not compact or compact in {"-", ".", "-."}:
        return None
    try:
        return float(compact)
    except Exception:
        return None


def resolve_performance_worksheet(spreadsheet):
    preferred = [
        "Báo cáo hiệu suất",
        "Bao cao hieu suat",
        "BÁO CÁO HIỆU SUẤT",
        "Hiệu suất",
        "Hieu suat",
        "Bảng hiệu suất",
        "Performance",
        "TỔNG",
        "Tong",
    ]
    for title in preferred:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue

    worksheets = spreadsheet.worksheets()
    if worksheets:
        return worksheets[0]
    raise gspread.exceptions.WorksheetNotFound("Hiệu suất")


def extract_metric_values(rows: list, aliases: list) -> tuple[float, float]:
    month_val = None
    day_val = None
    for row in rows:
        row_text = " ".join(str(cell or "") for cell in row)
        row_norm = normalize_sheet_tab_name(row_text)
        if not any(alias in row_norm for alias in aliases):
            continue

        numeric_values = [num for num in (parse_number_like(cell) for cell in row) if num is not None]
        if not numeric_values:
            continue

        is_day_row = any(key in row_norm for key in ["homnay", "today", "trongngay"])
        is_month_row = any(key in row_norm for key in ["thang", "month"])

        if is_day_row and day_val is None:
            day_val = numeric_values[0]
            continue

        if is_month_row and month_val is None:
            month_val = numeric_values[0]
            if len(numeric_values) > 1 and day_val is None:
                day_val = numeric_values[1]
            continue

        if month_val is None:
            month_val = numeric_values[0]
        if day_val is None and len(numeric_values) > 1:
            day_val = numeric_values[1]

    if month_val is None:
        month_val = 0.0
    if day_val is None:
        day_val = 0.0
    return float(month_val), float(day_val)


# Column header aliases for column-based "Báo cáo hiệu suất" sheet format
_COL_ALIASES = {
    "spend":   ["chiphiads", "tongtienchi", "chiphiquangcao", "tongspend", "chiphi"],
    "results": [
        "soluongdata", "soluongketqua", "sodata", "tongketqua", "data", "ketqua",
        "donhang", "tongdon", "sodon", "soluong", "tongkq", "tongketqua",
        "dondat", "dondathoa", "soketqua",
    ],
    "cpr":     ["giaso", "chiphidata", "chiphiketqua", "cpkq"],
    "ads_pct": ["phantramads", "tyleads", "percentads", "cpads", "pctads", "tylechiphi"],
    "aov":     ["giatritbon", "giatritbdon", "giatritrungbinhdon", "giatridon"],
    "doanh_so": ["doanhso", "doanhthu", "revenue", "tongdoanhthu", "tongthu", "tongdoanh"],
}


def _match_col_alias(header: str, key: str) -> bool:
    norm = normalize_sheet_tab_name(header)
    return any(alias in norm for alias in _COL_ALIASES[key])


def _extract_col_idx(header_row: list) -> dict:
    """Return dict of metric key → column index, or empty if not found."""
    idx = {}
    for i, cell in enumerate(header_row):
        for key in _COL_ALIASES:
            if key not in idx and _match_col_alias(str(cell), key):
                idx[key] = i
    return idx


def _get_cell(row: list, idx: int) -> float:
    if idx is None or idx >= len(row):
        return 0.0
    v = parse_number_like(row[idx])
    return float(v) if v is not None else 0.0


def fetch_performance_summary_column_based(rows: list) -> dict | None:
    """Parse sheet with column headers (e.g. Báo cáo hiệu suất tab).
    Returns metric dict or None if sheet doesn't match this format."""
    from datetime import date
    today_str = date.today().strftime("%d/%m/%Y")  # e.g. 28/04/2026

    # Find header row: prefer rows that expose core performance columns.
    header_row_idx = None
    col_idx = {}
    for i, row in enumerate(rows):
        cand = _extract_col_idx(row)
        has_main = "spend" in cand or "doanh_so" in cand
        has_support = "results" in cand or "cpr" in cand or "aov" in cand
        if has_main and has_support:
            header_row_idx = i
            col_idx = cand
            break

    if header_row_idx is None:
        return None

    # Find "Tổng" row (month totals) and today's row
    tong_row = None
    today_row = None
    for row in rows[header_row_idx + 1:]:
        if not row:
            continue
        first = normalize_sheet_tab_name(str(row[0]))
        if tong_row is None and first in ("tong", "tongsong", "tongcong"):
            tong_row = row
        if today_row is None and str(row[0]).strip() == today_str:
            today_row = row

    def extract(row, key):
        if row is None or key not in col_idx:
            return 0.0
        return _get_cell(row, col_idx[key])

    return {
        "spend_month":   extract(tong_row, "spend"),
        "spend_day":     extract(today_row, "spend"),
        "doanh_so_month": extract(tong_row, "doanh_so"),
        "doanh_so_day":   extract(today_row, "doanh_so"),
        "result_month":  extract(tong_row, "results"),
        "result_day":    extract(today_row, "results"),
        "cpr_month":     extract(tong_row, "cpr"),
        "cpr_day":       extract(today_row, "cpr"),
        "ads_month":     extract(tong_row, "ads_pct"),
        "ads_day":       extract(today_row, "ads_pct"),
        "aov_month":     extract(tong_row, "aov"),
        "aov_day":       extract(today_row, "aov"),
    }


def _parse_date_flexible(raw: str):
    """Try to parse a date string in multiple common formats. Returns datetime or None."""
    raw = raw.strip()
    for fmt in ("%d/%m/%Y", "%-d/%-m/%Y", "%d/%m/%y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except Exception:
            pass
    # Try without zero-padding via manual split  e.g. "1/4/2026"
    parts = raw.split("/")
    if len(parts) == 3:
        try:
            d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
            if y < 100:
                y += 2000
            return datetime(y, m, d)
        except Exception:
            pass
    return None


def fetch_performance_weekly_trend(rows: list) -> list:
    # --- Phase 1: try alias-based header detection ---
    header_row_idx = None
    col_idx = {}
    for i, row in enumerate(rows):
        cand = _extract_col_idx(row)
        has_result = "results" in cand
        has_revenue = "doanh_so" in cand or "spend" in cand
        if has_result and has_revenue:
            header_row_idx = i
            col_idx = cand
            break
        if has_result and header_row_idx is None:
            header_row_idx = i
            col_idx = cand

    # --- Phase 2: fallback — scan ALL rows for date+numbers pattern ---
    if header_row_idx is None:
        date_row_indices = []
        for i, row in enumerate(rows):
            if not row:
                continue
            cell0 = str(row[0] if len(row) > 0 else "").strip()
            if _parse_date_flexible(cell0) is not None:
                date_row_indices.append(i)

        if date_row_indices:
            first_date_idx = date_row_indices[0]
            if first_date_idx > 0:
                col_idx = _extract_col_idx(rows[first_date_idx - 1])

            series = []
            for i in date_row_indices:
                row = rows[i]
                raw_date = str(row[0]).strip()
                day_obj = _parse_date_flexible(raw_date)
                if day_obj is None:
                    continue

                nums = []
                for cell in row[1:]:
                    v = parse_number_like(str(cell))
                    if v is not None:
                        nums.append(float(v))

                results_val = _get_cell(row, col_idx.get("results")) if col_idx.get("results") else (nums[0] if nums else 0)
                revenue_val = _get_cell(row, col_idx.get("doanh_so")) if col_idx.get("doanh_so") else (nums[1] if len(nums) > 1 else 0)
                ads_val = _get_cell(row, col_idx.get("ads_pct")) if col_idx.get("ads_pct") else 0.0
                series.append(
                    {
                        "date": day_obj.strftime("%d/%m/%Y"),
                        "date_key": day_obj.strftime("%Y-%m-%d"),
                        "data": round(results_val),
                        "revenue": round(revenue_val),
                        "ads_percent": round(ads_val, 2),
                    }
                )

            series.sort(key=lambda item: item.get("date_key", ""))
            by_key = {item["date_key"]: item for item in series}
            window = []
            for offset in range(6, -1, -1):
                day = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
                if day in by_key:
                    window.append(by_key[day])
                else:
                    label = (datetime.now() - timedelta(days=offset)).strftime("%d/%m/%Y")
                    window.append({"date": label, "date_key": day, "data": 0, "revenue": 0, "ads_percent": 0.0})
            return window
        return []

    # --- Phase 1 succeeded: use detected header + col_idx ---
    series = []
    for row in rows[header_row_idx + 1:]:
        if not row:
            continue
        raw_date = str(row[0] if len(row) > 0 else "").strip()
        day_obj = _parse_date_flexible(raw_date)
        if day_obj is None:
            continue

        series.append(
            {
                "date": day_obj.strftime("%d/%m/%Y"),
                "date_key": day_obj.strftime("%Y-%m-%d"),
                "data": round(_get_cell(row, col_idx.get("results"))),
                "revenue": round(_get_cell(row, col_idx.get("doanh_so"))),
                "ads_percent": round(_get_cell(row, col_idx.get("ads_pct")), 2),
            }
        )

    series.sort(key=lambda item: item.get("date_key", ""))
    by_key = {item["date_key"]: item for item in series}
    window = []
    for offset in range(6, -1, -1):
        day = (datetime.now() - timedelta(days=offset)).strftime("%Y-%m-%d")
        if day in by_key:
            window.append(by_key[day])
        else:
            label = (datetime.now() - timedelta(days=offset)).strftime("%d/%m/%Y")
            window.append({"date": label, "date_key": day, "data": 0, "revenue": 0, "ads_percent": 0.0})
    return window


def _extract_fixed_summary_values(rows: list, col_idx: int) -> tuple[float, float]:
    if not rows:
        return 0.0, 0.0

    today = date.today()
    month_val = None
    day_val = None

    for row in rows:
        if not row:
            continue

        first_cell = str(row[0] if len(row) > 0 else "").strip()
        row_text = " ".join(str(cell or "") for cell in row)
        row_key = normalize_sheet_tab_name(first_cell)
        row_text_key = normalize_sheet_tab_name(row_text)
        row_value = _get_cell(row, col_idx)

        if month_val is None and (
            row_key in {"tong", "tongcong", "tongso", "total"}
            or any(token in row_text_key for token in ["tong", "tongcong", "tongso", "total"])
        ):
            month_val = row_value

        if day_val is None:
            parsed_day = _parse_date_flexible(first_cell)
            if parsed_day and parsed_day.date() == today:
                day_val = row_value

        if month_val is not None and day_val is not None:
            break

    if month_val is None:
        for row in rows:
            if not row:
                continue
            first_cell = str(row[0] if len(row) > 0 else "").strip()
            if _parse_date_flexible(first_cell) is not None:
                month_val = _get_cell(row, col_idx)
                break

    if month_val is None:
        for row in rows:
            if not row:
                continue
            candidate = _get_cell(row, col_idx)
            if candidate != 0:
                month_val = candidate
                break

    return float(month_val or 0.0), float(day_val or 0.0)


def fetch_performance_summary_fixed_columns(tong_rows: list, lng_rows: list) -> dict:
    # Fixed mapping requested by user.
    result_month, result_day = _extract_fixed_summary_values(tong_rows, 1)  # B
    doanh_so_month, doanh_so_day = _extract_fixed_summary_values(tong_rows, 2)  # C
    spend_month, spend_day = _extract_fixed_summary_values(tong_rows, 3)  # D
    ads_month, ads_day = _extract_fixed_summary_values(tong_rows, 4)  # E
    aov_month, aov_day = _extract_fixed_summary_values(tong_rows, 6)  # G

    completion_month, completion_day = _extract_fixed_summary_values(lng_rows, 9)  # J
    gross_month, gross_day = _extract_fixed_summary_values(lng_rows, 20)  # U
    gross_pct_month, gross_pct_day = _extract_fixed_summary_values(lng_rows, 21)  # V

    cpr_month = (spend_month / result_month) if result_month > 0 else 0.0
    cpr_day = (spend_day / result_day) if result_day > 0 else 0.0

    return {
        "spend_month": spend_month,
        "spend_day": spend_day,
        "doanh_so_month": doanh_so_month,
        "doanh_so_day": doanh_so_day,
        "result_month": result_month,
        "result_day": result_day,
        "cpr_month": cpr_month,
        "cpr_day": cpr_day,
        "ads_month": ads_month,
        "ads_day": ads_day,
        "aov_month": aov_month,
        "aov_day": aov_day,
        "completion_month": completion_month,
        "completion_day": completion_day,
        "gross_month": gross_month,
        "gross_day": gross_day,
        "gross_pct_month": gross_pct_month,
        "gross_pct_day": gross_pct_day,
    }


def extract_product_realtime_from_tong_rows(rows: list) -> dict:
    if not rows:
        return {"items": [], "top5": [], "date_from": "", "date_to": ""}

    date_from = ""
    date_to = ""
    if len(rows) > 0:
        top_row = rows[0]
        if len(top_row) > 2:
            date_from = str(top_row[2] or "").strip()
        if len(top_row) > 3:
            date_to = str(top_row[3] or "").strip()

    header_idx = None
    for idx, row in enumerate(rows[:12]):
        if len(row) < 8:
            continue
        header_text = normalize_sheet_tab_name(" ".join(str(cell or "") for cell in row))
        if "tenspvietnam" in header_text and "sodatara" in header_text:
            header_idx = idx
            break

    if header_idx is None:
        return {"items": [], "top5": [], "date_from": date_from, "date_to": date_to}

    items = []
    for row in rows[header_idx + 1 :]:
        if not row or len(row) < 8:
            continue

        name_vn = str(row[0] or "").strip()
        if not name_vn:
            continue
        name_key = normalize_sheet_tab_name(name_vn)
        if name_key in {"tong", "tongcong", "total"}:
            continue

        data_out = float(parse_number_like(row[2]) or 0)
        revenue = float(parse_number_like(row[3]) or 0)
        spend = float(parse_number_like(row[4]) or 0)
        ads_percent = float(parse_number_like(row[5]) or 0)
        stock = float(parse_number_like(row[6]) or 0)
        lng_percent = float(parse_number_like(row[7]) or 0)

        items.append(
            {
                "name_vn": name_vn,
                "data_out": data_out,
                "revenue": revenue,
                "spend": spend,
                "ads_percent": ads_percent,
                "stock": stock,
                "lng_percent": lng_percent,
            }
        )

    items.sort(key=lambda item: (item.get("data_out", 0), item.get("revenue", 0), -item.get("spend", 0)), reverse=True)
    positive_top = [item for item in items if float(item.get("data_out", 0)) > 0]
    top5 = (positive_top or items)[:5]

    return {
        "items": items,
        "top5": top5,
        "date_from": date_from,
        "date_to": date_to,
    }


def fetch_performance_summary(performance_sheet_url: str) -> dict:
    sheet_id = extract_sheet_id(performance_sheet_url)
    if not sheet_id:
        return {"success": False, "error": "Link bảng hiệu suất không hợp lệ."}

    client = get_gspread_client()
    spreadsheet = client.open_by_key(sheet_id)
    tong_ws = resolve_optional_worksheet(spreadsheet, ["TỔNG", "Tong", "Tổng"]) or resolve_performance_worksheet(spreadsheet)

    rows = tong_ws.get_all_values()
    product_rows = []
    try:
        product_rows = tong_ws.get("AA1:AH400")
    except Exception:
        product_rows = []

    product_realtime = extract_product_realtime_from_tong_rows(product_rows)
    tiktok_ws = resolve_optional_worksheet(spreadsheet, ["Tiktok", "TikTok", "TIKTOK"])
    lng_ws = resolve_optional_worksheet(spreadsheet, ["LNG", "LNG Sản phẩm", "LNG San pham", "LN gộp dự tính"])

    if not rows:
        return {"success": False, "error": "Bảng hiệu suất chưa có dữ liệu."}

    weekly_rows = rows
    if tiktok_ws is not None:
        try:
            tiktok_rows = tiktok_ws.get_all_values()
            if tiktok_rows:
                weekly_rows = tiktok_rows
        except Exception:
            pass

    lng_summary = {"items": []}
    lng_rows = []
    if lng_ws is not None:
        try:
            lng_rows = lng_ws.get_all_values()
            lng_summary = build_lng_items_from_rows(lng_rows)
        except Exception:
            lng_rows = []
            lng_summary = {"items": []}

    fixed_result = fetch_performance_summary_fixed_columns(rows, lng_rows)
    col_result = fetch_performance_summary_column_based(rows)

    fixed_core_sum = (
        abs(fixed_result["spend_month"])
        + abs(fixed_result["doanh_so_month"])
        + abs(fixed_result["result_month"])
        + abs(fixed_result["ads_month"])
        + abs(fixed_result["aov_month"])
    )

    if fixed_core_sum > 0 or not col_result:
        spend_month = fixed_result["spend_month"]
        spend_day = fixed_result["spend_day"]
        doanh_so_month = fixed_result["doanh_so_month"]
        doanh_so_day = fixed_result["doanh_so_day"]
        result_month = fixed_result["result_month"]
        result_day = fixed_result["result_day"]
        cpr_month = fixed_result["cpr_month"]
        cpr_day = fixed_result["cpr_day"]
        ads_month = fixed_result["ads_month"]
        ads_day = fixed_result["ads_day"]
        aov_month = fixed_result["aov_month"]
        aov_day = fixed_result["aov_day"]
    else:
        spend_month = col_result["spend_month"]
        spend_day = col_result["spend_day"]
        doanh_so_month = col_result["doanh_so_month"]
        doanh_so_day = col_result["doanh_so_day"]
        result_month = col_result["result_month"]
        result_day = col_result["result_day"]
        cpr_month = col_result["cpr_month"]
        cpr_day = col_result["cpr_day"]
        ads_month = col_result["ads_month"]
        ads_day = col_result["ads_day"]
        aov_month = col_result["aov_month"]
        aov_day = col_result["aov_day"]

    completion_payload = {
        "total": round(fixed_result["completion_month"], 2),
        "day": round(fixed_result["completion_day"], 2),
        "unit": "%",
    }
    gross_payload = {
        "total": round(fixed_result["gross_month"]),
        "day": round(fixed_result["gross_day"]),
        "unit": "VND",
    }
    gross_pct_payload = {
        "total": round(fixed_result["gross_pct_month"], 2),
        "day": round(fixed_result["gross_pct_day"], 2),
        "unit": "%",
    }

    weekly_trend = fetch_performance_weekly_trend(weekly_rows)
    if not weekly_trend and weekly_rows is not rows:
        weekly_trend = fetch_performance_weekly_trend(rows)

    return {
        "success": True,
        "metrics": {
            "revenue": {"month": round(doanh_so_month), "day": round(doanh_so_day), "unit": "VND"},
            "total_spend": {"month": round(spend_month), "day": round(spend_day), "unit": "VND"},
            "total_results": {"month": round(result_month), "day": round(result_day), "unit": "data"},
            "cost_per_result": {"month": round(cpr_month), "day": round(cpr_day), "unit": "VND"},
            "ads_percent": {"month": round(ads_month, 2), "day": round(ads_day, 2), "unit": "%"},
            "avg_order_value": {"month": round(aov_month), "day": round(aov_day), "unit": "VND"},
            "completion_percent": completion_payload,
            "gross_profit": gross_payload,
            "gross_profit_percent": gross_pct_payload,
            "weekly_trend": weekly_trend,
            "product_lng": lng_summary,
            "product_realtime": product_realtime,
        },
        "sheet_name": spreadsheet.title or "Bảng hiệu suất",
    }

DISPLAY_COLUMNS = ["Ngày", "Tên tài khoản", "Tên sản phẩm - VN", "Số Data", "Số tiền chi tiêu - VND", "Số tiền chi tiêu - USD"]


def normalize_sheet_tab_name(value: str) -> str:
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    return re.sub(r"[^a-z0-9]", "", text)


def resolve_ads_worksheet(spreadsheet):
    # Try strict names first to preserve current behavior.
    preferred_titles = [
        "Chi phí ads FB",
        "Chi Phi ads FB",
        "Chi phi ads FB",
        "Data FB",
        "Chi phí ADS",
        "Chi phí Ads",
        "CP ADS-Chuyển đổi",
        "CP ADS - Chuyển đổi",
        "CP ADS Chuyển đổi",
        "CP ADS",
        "Chi Phi ADS",
        "Chi Phi Ads",
        "Chi phi ADS",
        "Chi phi Ads",
    ]
    for title in preferred_titles:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue

    # Fuzzy fallback: match tabs that look like "chi phi ads".
    candidates = []
    try:
        for ws in spreadsheet.worksheets():
            norm = normalize_sheet_tab_name(ws.title)
            has_ads = "ads" in norm
            looks_like_chi_phi_ads = ("chiphi" in norm or "chiph" in norm)
            looks_like_cp_ads = norm.startswith("cpads") or "cpads" in norm
            if has_ads and (looks_like_chi_phi_ads or looks_like_cp_ads):
                candidates.append(ws)
    except Exception:
        candidates = []

    if candidates:
        return candidates[0]

    raise gspread.exceptions.WorksheetNotFound("Chi phí ADS")


DISPLAY_COLUMN_ALIASES = {
    "Ngày": ["ngay", "date"],
    "Tên tài khoản": ["tentaikhoan", "tai khoan", "account"],
    "Tên sản phẩm - VN": ["tensanphamvn", "tenspvn", "tensanpham", "sanpham"],
    "Số Data": ["sodata", "tongdata", "tongketqua", "ketqua"],
    "Số tiền chi tiêu - VND": ["sotienchitieuvnd", "chiphivnd", "tongchiphi", "spendvnd", "vnd"],
    "Số tiền chi tiêu - USD": ["sotienchitieuusd", "chiphiusd", "spendusd", "usd"],
}


def _match_display_header(header: str, col_name: str) -> bool:
    norm = normalize_sheet_tab_name(header)
    aliases = DISPLAY_COLUMN_ALIASES.get(col_name, [])
    return any(alias in norm for alias in aliases)


def _extract_display_col_idx(header_row: list) -> dict:
    idx = {}
    for i, cell in enumerate(header_row):
        cell_text = str(cell or "")
        for col in DISPLAY_COLUMNS:
            if col not in idx and _match_display_header(cell_text, col):
                idx[col] = i
    return idx


def _find_ads_header_row(all_values: list) -> tuple[int, dict]:
    scan_limit = min(len(all_values), 15)
    for row_idx in range(scan_limit):
        row = all_values[row_idx] if row_idx < len(all_values) else []
        col_idx = _extract_display_col_idx(row)
        has_core = "Ngày" in col_idx and ("Số Data" in col_idx or "Số tiền chi tiêu - VND" in col_idx)
        if len(col_idx) >= 4 and has_core:
            return row_idx, col_idx
    fallback_idx = _extract_display_col_idx(all_values[0] if all_values else [])
    return 0, fallback_idx


def _parse_ads_rows_from_worksheet(worksheet) -> list:
    all_values = worksheet.get_all_values()
    if not all_values:
        return []

    header_row_idx, col_idx = _find_ads_header_row(all_values)
    if not col_idx or "Ngày" not in col_idx:
        return []

    rows = []
    for i in range(header_row_idx + 1, len(all_values)):
        row = all_values[i]
        if not row:
            continue

        first_cell = str(row[0] if row else "").strip()
        first_norm = normalize_sheet_tab_name(first_cell)
        if any(token in first_norm for token in ["tong", "tongcong", "total"]) and not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", first_cell):
            continue

        row_data = {}
        has_content = False
        for col in DISPLAY_COLUMNS:
            idx = col_idx.get(col)
            value = str(row[idx]).strip() if idx is not None and idx < len(row) else ""
            if "#N/A" in value or "#REF" in value or "#VALUE" in value:
                value = ""
            if value:
                has_content = True
            row_data[col] = value

        if not has_content:
            continue

        date_text = str(row_data.get("Ngày", "")).strip()
        parsed_date = _parse_date_flexible(date_text)
        if parsed_date is None:
            for col in DISPLAY_COLUMNS:
                candidate = str(row_data.get(col, "")).strip()
                candidate_date = _parse_date_flexible(candidate)
                if candidate_date is not None:
                    parsed_date = candidate_date
                    row_data["Ngày"] = candidate_date.strftime("%d/%m/%Y")
                    if col == "Số Data":
                        row_data["Số Data"] = ""
                    break

        if parsed_date is None:
            continue

        has_numeric_metric = any(
            parse_number_like(str(row_data.get(col, ""))) is not None
            for col in ["Số Data", "Số tiền chi tiêu - VND", "Số tiền chi tiêu - USD"]
        )
        if not has_numeric_metric:
            continue

        rows.append(row_data)

    # Fallback for sheets that keep data in fixed columns but have non-standard headers.
    if rows:
        return rows

    for row in all_values:
        if not row:
            continue

        parsed_date = None
        date_text = ""
        for candidate in row[:4]:
            candidate_text = str(candidate or "").strip()
            candidate_date = _parse_date_flexible(candidate_text)
            if candidate_date is not None:
                parsed_date = candidate_date
                date_text = candidate_date.strftime("%d/%m/%Y")
                break

        if parsed_date is None:
            continue

        account_text = str(row[1] if len(row) > 1 else "").strip()
        product_text = str(row[2] if len(row) > 2 else "").strip()
        data_text = str(row[3] if len(row) > 3 else "").strip()
        spend_vnd_text = str(row[4] if len(row) > 4 else "").strip()
        spend_usd_text = str(row[5] if len(row) > 5 else "").strip()

        has_metric = any(
            parse_number_like(val) is not None
            for val in [data_text, spend_vnd_text, spend_usd_text]
        )
        if not has_metric:
            continue

        rows.append(
            {
                "Ngày": date_text,
                "Tên tài khoản": account_text,
                "Tên sản phẩm - VN": product_text,
                "Số Data": data_text,
                "Số tiền chi tiêu - VND": spend_vnd_text,
                "Số tiền chi tiêu - USD": spend_usd_text,
            }
        )

    return rows


def resolve_ads_worksheets(spreadsheet) -> list:
    preferred_titles = [
        "Chi phí ads FB",
        "Chi Phi ads FB",
        "Chi phi ads FB",
        "Data FB",
    ]
    found = []
    seen = set()

    for title in preferred_titles:
        try:
            ws = spreadsheet.worksheet(title)
            key = normalize_sheet_tab_name(ws.title)
            if key and key not in seen:
                seen.add(key)
                found.append(ws)
        except Exception:
            continue

    if found:
        return found

    # Backward-compatible fallback for older sheet naming.
    return [resolve_ads_worksheet(spreadsheet)]


def resolve_optional_worksheet(spreadsheet, preferred_titles: list[str]):
    for title in preferred_titles:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue
    return None


def extract_member_matrix_summary(spreadsheet) -> list:
    data_ws = resolve_optional_worksheet(spreadsheet, ["Data FB"])
    if data_ws is None:
        return []

    try:
        data_values = data_ws.get_all_values()
    except Exception:
        return []

    if len(data_values) < 3:
        return []

    header_row = data_values[1] if len(data_values) > 1 else []
    total_row = data_values[2] if len(data_values) > 2 else []

    cost_map = {}
    cost_ws = resolve_optional_worksheet(spreadsheet, ["Chi phí ads FB", "Chi Phi ads FB", "Chi phi ads FB"])
    if cost_ws is not None:
        try:
            cost_values = cost_ws.get_all_values()
            if len(cost_values) >= 6:
                cost_header = cost_values[4]
                cost_total = cost_values[5]
                for idx in range(1, min(len(cost_header), len(cost_total))):
                    member_name = str(cost_header[idx] or "").strip()
                    if not member_name:
                        continue
                    cost_val = parse_number_like(str(cost_total[idx] or ""))
                    if cost_val is not None:
                        cost_map[member_name] = float(cost_val)
        except Exception:
            pass

    summaries = []
    for idx in range(1, min(len(header_row), len(total_row))):
        member_name = str(header_row[idx] or "").strip()
        if not member_name or normalize_sheet_tab_name(member_name) in {"tennv", "tong"}:
            continue

        total_data_val = parse_number_like(str(total_row[idx] or ""))
        total_data = round(float(total_data_val)) if total_data_val is not None else 0
        if total_data <= 0:
            continue

        summaries.append(
            {
                "name": member_name,
                "team": "",
                "total_spend": 0,
                "total_data": total_data,
                "cost_per_data": 0,
                "ads_percent": round(cost_map.get(member_name, 0.0), 2),
            }
        )

    summaries.sort(key=lambda x: x.get("total_data", 0), reverse=True)
    return summaries


def build_lng_items_from_rows(rows: list) -> dict:
    if not rows:
        return {"items": []}

    header_idx = None
    product_idx = None
    lng_idx = None
    lng_pct_idx = None

    scan_limit = min(len(rows), 12)
    for i in range(scan_limit):
        row = rows[i]
        normalized = [normalize_sheet_tab_name(cell) for cell in row]
        for j, cell in enumerate(normalized):
            if product_idx is None and ("sanpham" in cell or "tensp" in cell):
                product_idx = j
            if lng_idx is None and ("lng" in cell or "loinhuangop" in cell or "lngop" in cell):
                lng_idx = j
            if lng_pct_idx is None and ("phantramlng" in cell or "phantramln" in cell or ("phantram" in cell and "lng" in cell)):
                lng_pct_idx = j
        if product_idx is not None and lng_idx is not None:
            header_idx = i
            break

    if header_idx is None or product_idx is None or lng_idx is None:
        return {"items": []}

    items = []
    for row in rows[header_idx + 1:]:
        if not row:
            continue
        name = str(row[product_idx] if product_idx < len(row) else "").strip()
        if not name:
            continue
        name_norm = normalize_sheet_tab_name(name)
        if any(token in name_norm for token in ["tong", "tongcong", "total"]):
            continue

        lng_raw = row[lng_idx] if lng_idx < len(row) else ""
        lng_val = parse_number_like(str(lng_raw))
        if lng_val is None:
            continue

        lng_pct_val = None
        if lng_pct_idx is not None and lng_pct_idx < len(row):
            pct_raw = parse_number_like(str(row[lng_pct_idx]))
            if pct_raw is not None:
                lng_pct_val = round(float(pct_raw), 2)

        items.append({"product_name": name, "lng": round(float(lng_val)), "lng_pct": lng_pct_val})

    items.sort(key=lambda x: float(x.get("lng", 0)), reverse=True)
    return {"items": items}


def resolve_profitability_worksheet(spreadsheet):
    preferred_titles = [
        "LN gộp dự tính",
        "LN gop du tinh",
        "Lợi nhuận gộp dự tính",
        "Loi nhuan gop du tinh",
    ]

    for title in preferred_titles:
        try:
            return spreadsheet.worksheet(title)
        except Exception:
            continue

    candidates = []
    try:
        for ws in spreadsheet.worksheets():
            norm = normalize_sheet_tab_name(ws.title)
            if "lngop" in norm or "loinhuangop" in norm:
                candidates.append(ws)
    except Exception:
        candidates = []

    if candidates:
        return candidates[0]

    return None


def fetch_profitability_summary(spreadsheet) -> dict:
    worksheet = resolve_profitability_worksheet(spreadsheet)
    if worksheet is None:
        return {
            "completion_percent": {"total": 0.0, "unit": "%"},
            "gross_profit": {"total": 0.0, "unit": "VND"},
            "gross_profit_percent": {"total": 0.0, "unit": "%"},
        }

    try:
        rows = worksheet.get_all_values()
    except Exception:
        rows = []

    if not rows:
        return {
            "completion_percent": {"total": 0.0, "unit": "%"},
            "gross_profit": {"total": 0.0, "unit": "VND"},
            "gross_profit_percent": {"total": 0.0, "unit": "%"},
        }

    completion_col_idx = 9  # J: dữ liệu tổng ngay dưới "Tỷ lệ hoàn dự tính"
    gross_profit_col_idx = 20  # U: Lợi nhuận gộp
    gross_profit_pct_col_idx = 21  # V: %LN gộp

    def get_numeric_at(row: list, col_idx: int) -> float | None:
        if col_idx >= len(row):
            return None
        parsed = parse_number_like(row[col_idx])
        return float(parsed) if parsed is not None else None

    def is_total_row(row: list) -> bool:
        text = " ".join(str(cell or "") for cell in row[:4])
        norm = normalize_sheet_tab_name(text)
        return any(key in norm for key in ["tong", "tongcong", "tongket", "total"])

    def find_value_below_label(target_col_idx: int, label_keywords: list[str]) -> float | None:
        for row_index, row in enumerate(rows):
            if target_col_idx >= len(row):
                continue
            cell_text = normalize_sheet_tab_name(row[target_col_idx])
            if not cell_text:
                continue
            if all(keyword in cell_text for keyword in label_keywords):
                for next_row in rows[row_index + 1: row_index + 6]:
                    val = get_numeric_at(next_row, target_col_idx)
                    if val is not None:
                        return val
        return None

    total_row = next((row for row in rows if is_total_row(row)), None)

    completion_total = find_value_below_label(completion_col_idx, ["ty", "le", "hoan", "du", "tinh"])
    if completion_total is None:
        completion_total = get_numeric_at(total_row, completion_col_idx) if total_row else None
    gross_total = get_numeric_at(total_row, gross_profit_col_idx) if total_row else None
    gross_pct_total = get_numeric_at(total_row, gross_profit_pct_col_idx) if total_row else None

    # Fallback: if no explicit total row, use the last numeric value in each target column.
    if completion_total is None:
        for row in reversed(rows):
            completion_total = get_numeric_at(row, completion_col_idx)
            if completion_total is not None:
                break

    if gross_total is None:
        for row in reversed(rows):
            gross_total = get_numeric_at(row, gross_profit_col_idx)
            if gross_total is not None:
                break

    if gross_pct_total is None:
        for row in reversed(rows):
            gross_pct_total = get_numeric_at(row, gross_profit_pct_col_idx)
            if gross_pct_total is not None:
                break

    completion_total = float(completion_total or 0.0)
    gross_total = float(gross_total or 0.0)
    gross_pct_total = float(gross_pct_total or 0.0)

    return {
        "completion_percent": {"total": round(completion_total, 2), "unit": "%"},
        "gross_profit": {"total": round(gross_total), "unit": "VND"},
        "gross_profit_percent": {"total": round(gross_pct_total, 2), "unit": "%"},
    }


def build_account_spend_summary(rows: list) -> list:
    today_key = datetime.now().strftime("%d/%m/%Y")
    by_account = {}
    for row in rows:
        account_name = (row.get("Tên tài khoản") or "").strip() or "Không rõ tài khoản"
        spend = parse_spend(row.get("Số tiền chi tiêu - VND", ""))
        date_text = (row.get("Ngày") or "").strip()

        if account_name not in by_account:
            by_account[account_name] = {
                "account_name": account_name,
                "total_spend": 0.0,
                "today_spend": 0.0,
            }

        by_account[account_name]["total_spend"] += spend
        if date_text == today_key:
            by_account[account_name]["today_spend"] += spend

    summaries = []
    for item in by_account.values():
        total = float(item.get("total_spend", 0.0))
        today = float(item.get("today_spend", 0.0))
        summaries.append(
            {
                "account_name": item.get("account_name", ""),
                "total_spend": round(total),
                "today_spend": round(today),
                "is_live": today > 0,
            }
        )

    summaries.sort(key=lambda x: float(x.get("total_spend", 0)), reverse=True)
    return summaries


def build_product_lng_summary(spreadsheet) -> dict:
    worksheet = resolve_profitability_worksheet(spreadsheet)
    if worksheet is None:
        return {"top": [], "bottom": []}

    try:
        rows = worksheet.get_all_values()
    except Exception:
        rows = []

    if not rows:
        return {"top": [], "bottom": []}

    gross_col_idx = 20  # U
    product_col_idx = 3  # default D (English name)
    vn_col_idx: int | None = None  # Vietnamese name column, if separate
    header_idx = 0

    for i, row in enumerate(rows[:8]):
        normalized = [normalize_sheet_tab_name(cell) for cell in row]
        if any("sanpham" in cell or "tensp" in cell for cell in normalized):
            header_idx = i
            # Find any product name column
            all_product_idxs = [j for j, cell in enumerate(normalized)
                                 if "sanpham" in cell or "tensp" in cell]
            # Among those, prefer the one with "vn" or "viet"
            vn_candidates = [j for j in all_product_idxs
                             if "vn" in normalized[j] or "viet" in normalized[j]]
            # Also widen: any column ending with "vn" (e.g. "tensanphamvn")
            if not vn_candidates:
                vn_candidates = [j for j, cell in enumerate(normalized)
                                 if cell.endswith("vn") and len(cell) >= 4]
            en_candidates = [j for j in all_product_idxs if j not in vn_candidates]

            if vn_candidates:
                vn_col_idx = vn_candidates[0]
                product_col_idx = vn_col_idx
            elif en_candidates:
                product_col_idx = en_candidates[0]

            # If we found both en and vn separately, track en as well
            if vn_candidates and en_candidates:
                product_col_idx = vn_candidates[0]  # primary = VN
                # en kept separately for cross-reference if needed
            break

    # Also find % LNG column (e.g. "%LNG", "%LN", or column V index 21)
    lng_pct_col_idx: int | None = None
    if header_idx < len(rows):
        header_row_normalized = [normalize_sheet_tab_name(cell) for cell in rows[header_idx]]
        for j, cell in enumerate(header_row_normalized):
            if ("phantramlng" in cell or "phantramln" in cell or
                    ("phantram" in cell and ("gop" in cell or "ln" in cell or "lng" in cell))):
                lng_pct_col_idx = j
                break
        # Fallback: column V (index 21) which is the known %LN gộp column
        if lng_pct_col_idx is None and gross_col_idx == 20:
            lng_pct_col_idx = 21

    products = []
    for row in rows[header_idx + 1:]:
        if not row:
            continue
        name = str(row[product_col_idx] if product_col_idx < len(row) else "").strip()
        if not name:
            continue

        name_norm = normalize_sheet_tab_name(name)
        if any(token in name_norm for token in ["tong", "tongcong", "total"]):
            continue

        if gross_col_idx >= len(row):
            continue
        lng_val = parse_number_like(row[gross_col_idx])
        if lng_val is None:
            continue

        lng_pct_val = None
        if lng_pct_col_idx is not None and lng_pct_col_idx < len(row):
            raw_pct = parse_number_like(row[lng_pct_col_idx])
            if raw_pct is not None:
                lng_pct_val = round(float(raw_pct), 2)

        products.append({"product_name": name, "lng": round(float(lng_val)), "lng_pct": lng_pct_val})

    if not products:
        return {"items": []}

    desc = sorted(products, key=lambda x: float(x.get("lng", 0)), reverse=True)
    return {"items": desc}

def fetch_chi_phi_ads_data(sheet_id):
    """Fetch merged data from Chi phí ads FB + Data FB tabs (fallback to legacy ads tab)."""
    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        worksheets = resolve_ads_worksheets(spreadsheet)

        rows = []
        seen_keys = set()
        for ws in worksheets:
            parsed_rows = _parse_ads_rows_from_worksheet(ws)
            for row_data in parsed_rows:
                key = "|".join(str(row_data.get(col, "")).strip() for col in DISPLAY_COLUMNS)
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
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

        profitability_metrics = fetch_profitability_summary(spreadsheet)
        profitability_metrics["product_lng"] = build_product_lng_summary(spreadsheet)
        account_summary = build_account_spend_summary(rows)
        matrix_member_summary = extract_member_matrix_summary(spreadsheet)

        return {
            "success": True,
            "data": rows,
            "headers": DISPLAY_COLUMNS,
            "ads_percent": ads_percent,
            "profitability_metrics": profitability_metrics,
            "account_summary": account_summary,
            "matrix_member_summary": matrix_member_summary,
        }
    except Exception as e:
        raw_error = str(e)
        lower_error = raw_error.lower()
        access_links = build_sheet_access_links(sheet_id)
        service_email = get_service_account_client_email()
        help_text, steps, _ = build_sheet_access_help(service_email, sheet_id)

        if "<response [404]>" in lower_error:
            return {
                "success": False,
                "error": "Không tìm thấy sheet hoặc hệ thống chưa được cấp quyền truy cập.",
                "help": help_text,
                "help_steps": steps,
                "service_account_email": service_email,
                "sheet_id": sheet_id,
                "clean_url": access_links.get("clean_url", ""),
                "share_url": access_links.get("share_url", ""),
                "request_access_url": access_links.get("request_access_url", ""),
                "can_auto_open_sheet": True,
            }

        if "permission" in lower_error or "forbidden" in lower_error or "<response [403]>" in lower_error:
            return {
                "success": False,
                "error": "Sheet chưa chia sẻ cho service account (Editor/Viewer).",
                "help": help_text,
                "help_steps": steps,
                "service_account_email": service_email,
                "sheet_id": sheet_id,
                "clean_url": access_links.get("clean_url", ""),
                "share_url": access_links.get("share_url", ""),
                "request_access_url": access_links.get("request_access_url", ""),
                "can_auto_open_sheet": True,
            }

        if (
            isinstance(e, gspread.exceptions.WorksheetNotFound)
            or "worksheetnotfound" in lower_error
            or (
                ("chiphi" in normalize_sheet_tab_name(raw_error) or "chiph" in normalize_sheet_tab_name(raw_error))
                and "ads" in normalize_sheet_tab_name(raw_error)
            )
        ):
            return {
                "success": False,
                "error": "Không tìm thấy tab dữ liệu hợp lệ trong sheet này.",
                "help_steps": [
                    "Mở file Google Sheet và kiểm tra tên tab chứa dữ liệu chi phí.",
                    "Đổi tên tab thành 'Chi phí ADS' hoặc 'CP ADS-Chuyển đổi' (hoặc tên gần giống có chứa 'ads').",
                    "Đảm bảo các cột bắt buộc có trong tab: Ngày, Tên tài khoản, Tên sản phẩm - VN, Số Data, Số tiền chi tiêu - VND.",
                ],
            }

        if "resource_exhausted" in lower_error or "quota exceeded" in lower_error or "rate_limit_exceeded" in lower_error:
            return {
                "success": False,
                "error": "Google Sheets đang tạm giới hạn số lần đọc dữ liệu (quota/phút). Vui lòng đợi 60-90 giây rồi bấm Tải Dữ Liệu lại.",
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
    performance_sheet_url = session.get("performance_sheet_url", "")
    is_elevated = bool(session.get("is_elevated", False))
    base_employee = get_base_employee_session()
    can_elevate = role == "employee" and not is_elevated
    can_view_team = role in {"lead", "admin"}
    can_manage_users = role == "admin"
    accessible_sheets = get_accessible_sheets_for_user(username)
    monthly_sheets = get_user_monthly_sheets(username, fallback_sheet_url=sheet_url)
    # Extract performance sheets from monthly sheets for autocomplete
    monthly_performance_sheets = [
        {
            "month_key": m["month_key"],
            "month_label": m["month_label"],
            "sheet_name": m.get("performance_sheet_name", "") or "Bảng hiệu suất",
            "sheet_url": m.get("performance_sheet_url", ""),
        }
        for m in monthly_sheets
        if m.get("performance_sheet_url", "")
    ]
    if performance_sheet_url and not any(item.get("sheet_url") == performance_sheet_url for item in monthly_performance_sheets):
        current_perf_name, _, _ = get_sheet_name_and_month(performance_sheet_url)
        monthly_performance_sheets.insert(0, {
            "month_key": current_month_key(),
            "month_label": month_label(current_month_key()),
            "sheet_name": current_perf_name or "Bảng hiệu suất hiện tại",
            "sheet_url": performance_sheet_url,
        })
    inline_style_css = read_web_static_asset("style.css")
    inline_script_js = read_web_static_asset("script.js")
    return render_template(
        "index.html",
        role=role,
        display_name=display_name,
        team=team,
        sheet_url=sheet_url,
        performance_sheet_url=performance_sheet_url,
        session_timeout_seconds=SESSION_TIMEOUT_SECONDS,
        is_elevated=is_elevated,
        base_employee=base_employee,
        can_elevate=can_elevate,
        can_view_team=can_view_team,
        can_manage_users=can_manage_users,
        accessible_sheets_count=len(accessible_sheets),
        accessible_sheets_json=json.dumps(accessible_sheets, ensure_ascii=False),
        monthly_sheets_json=json.dumps(monthly_sheets, ensure_ascii=False),
        monthly_performance_sheets_json=json.dumps(monthly_performance_sheets, ensure_ascii=False),
        inline_style_css=inline_style_css,
        inline_script_js=inline_script_js,
    )


@app.route("/admin/dashboard")
@login_required
def admin_dashboard():
    if ROLE_LEVELS.get(session.get("role", ""), 0) < ROLE_LEVELS["admin"]:
        return redirect(url_for("index"))
    return index()


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        if is_logged_in():
            return redirect(url_for("index"))
        success_message = ""
        error_message = ""
        next_target = request.args.get("next", "").strip()
        expired = request.args.get("expired", "").strip()
        registered = request.args.get("registered", "").strip()
        telegram_ready = request.args.get("telegram_ready", "").strip()
        telegram_test = request.args.get("telegram_test", "").strip()
        registered_username = request.args.get("username", "").strip()
        if expired == "1":
            error_message = "Phiên đăng nhập đã tự thoát sau 10 phút không thao tác. Vui lòng đăng nhập lại."
        if registered == "1":
            success_message = (
                (
                    f'Đăng ký thành công cho tài khoản "{registered_username}". Telegram đã lưu và gửi test thành công. Vui lòng đăng nhập để vào hệ thống.'
                    if telegram_test == "sent"
                    else f'Đăng ký thành công cho tài khoản "{registered_username}". Telegram đã lưu nhưng chưa gửi được tin test: Bot Token chưa hợp lệ.'
                    if telegram_test == "not_configured"
                    else f'Đăng ký thành công cho tài khoản "{registered_username}". Telegram đã lưu nhưng gửi test chưa thành công. Hãy kiểm tra Bot Token, bấm Start cho bot rồi thử lại.'
                    if telegram_test == "failed"
                    else f'Đăng ký thành công cho tài khoản "{registered_username}" và đã lưu Telegram. Vui lòng đăng nhập để vào hệ thống.'
                )
                if registered_username and telegram_ready == "1"
                else f'Đăng ký thành công cho tài khoản "{registered_username}". Vui lòng đăng nhập để vào hệ thống.'
                if registered_username
                else "Đăng ký thành công. Vui lòng đăng nhập để vào hệ thống."
            )
        return render_template(
            "login.html",
            error=error_message,
            success=success_message,
            title="Chi Phi Ads Dashboard",
            subtitle="Ae làm cái này thì sau không cần điền chi phí ads mỗi ngày nữa nè. Phêêêêêêêê...!",
            board_name=LOGIN_BOARD_NAME,
            form_action=url_for("login", next=next_target) if next_target else url_for("login"),
            submit_label="Đăng nhập",
            show_register_link=True,
            show_back_link=False,
            mode="employee",
        )

    username = normalize_username(request.form.get("username", ""))
    password = request.form.get("password", "")
    users_snapshot = load_users_config()
    username_key, user = get_user_entry(username, users_snapshot)

    if user and user.get("password") == password:
        next_url = get_safe_next_url(request.args.get("next", ""))
        role = user.get("role")
        if role == "employee":
            set_session_user(username_key or username, user, elevated=False)
            if employee_requires_telegram_setup(username, user):
                return redirect(url_for("employee_telegram_connect", next=next_url))
            return redirect(next_url)
        if role in {"lead", "admin"}:
            # Step 1: privileged accounts enter employee-mode dashboard.
            set_session_user(username_key or username, user, elevated=False, session_role="employee")
            return redirect(next_url)

    error_message = "Sai tài khoản hoặc mật khẩu"

    if not user:
        if not users_snapshot:
            error_message = "Hệ thống tài khoản đang tạm lỗi dữ liệu. Tài khoản không bị xóa, vui lòng liên hệ admin để khôi phục file users."
        else:
            error_message = "Tài khoản chưa tồn tại trong hệ thống"
    elif user.get("role") == "employee" and user.get("password") != password:
        error_message = "Tài khoản tồn tại nhưng mật khẩu chưa đúng. Nếu quên mật khẩu, bấm 'Quên mật khẩu'."
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
        form_action=url_for("login", next=request.args.get("next", "").strip()) if request.args.get("next", "").strip() else url_for("login"),
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

    username = normalize_username(request.form.get("username", ""))
    password = request.form.get("password", "")
    username_key, user = get_user_entry(username)

    if user and user.get("password") == password and user.get("role") in {"lead", "admin"}:
        set_session_user(username_key or username, user, elevated=True)
        if user.get("role") == "admin":
            return redirect(url_for("admin_dashboard"))
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
        username = str(session.get("username", "")).strip()
        users = load_users_config()
        user = users.get(username)
        if not user:
            session.clear()
            return redirect(url_for("login"))
        set_session_user(username, user, elevated=False, session_role="employee")

    return redirect(url_for("index"))


@app.route("/register", methods=["GET", "POST"])
def register_employee():
    if is_logged_in():
        return redirect(url_for("index"))

    form_values = {
        "username": "",
        "display_name": "",
        "team": "",
    }

    if request.method == "GET":
        return render_template(
            "register.html",
            error="",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    username = normalize_username(request.form.get("username", ""))
    display_name = request.form.get("display_name", "").strip()
    team = request.form.get("team", "").strip()
    password = request.form.get("password", "")
    confirm_password = request.form.get("confirm_password", "")

    form_values = {
        "username": username,
        "display_name": display_name,
        "team": team,
    }

    if not username or not display_name or not password or not confirm_password or not team:
        return render_template(
            "register.html",
            error="Vui lòng nhập đầy đủ thông tin đăng ký.",
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    if not re.fullmatch(r"[a-z0-9._-]{3,64}", username):
        return render_template(
            "register.html",
            error="Tên đăng nhập chỉ gồm chữ thường, số, dấu chấm, gạch dưới hoặc gạch ngang (3-64 ký tự).",
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

    users = load_users_config()
    existing_key, _existing_user = get_user_entry(username, users)
    if existing_key:
        return render_template(
            "register.html",
            error=f'Tên đăng nhập "{username}" đã tồn tại.',
            board_name=LOGIN_BOARD_NAME,
            team_codes=TEAM_CODES,
            form_values=form_values,
        )

    users[username] = {
        "password": password,
        "role": "employee",
        "team": team,
        "display_name": display_name,
        "telegram_verified": False,
        "telegram_test_status": "pending",
    }
    save_users_config(users)

    session["pending_telegram_setup"] = username
    return redirect(url_for("register_telegram"))


@app.route("/register/telegram", methods=["GET", "POST"])
def register_telegram():
    if is_logged_in():
        return redirect(url_for("index"))

    pending_username = str(session.get("pending_telegram_setup", "")).strip()
    if not pending_username:
        return redirect(url_for("register_employee"))

    users = load_users_config()
    user = users.get(pending_username)
    if not user or user.get("role") != "employee":
        session.pop("pending_telegram_setup", None)
        return redirect(url_for("register_employee"))

    form_values = {
        "telegram_chat_id": user.get("telegram_chat_id", ""),
        "telegram_username": user.get("telegram_username", ""),
        "telegram_bot_username": user.get("telegram_bot_username", ""),
        "telegram_bot_token": user.get("telegram_bot_token", ""),
    }

    if request.method == "GET":
        bind_code = ensure_telegram_bind_code(pending_username)
        return render_telegram_setup_page(
            error="",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=bind_code,
            form_action=url_for("register_telegram"),
        )

    telegram_chat_id = request.form.get("telegram_chat_id", "").strip()
    telegram_username = request.form.get("telegram_username", "").strip()
    telegram_bot_username = request.form.get("telegram_bot_username", "").strip()
    telegram_bot_token = request.form.get("telegram_bot_token", "").strip()
    form_values = {
        "telegram_chat_id": telegram_chat_id,
        "telegram_username": telegram_username,
        "telegram_bot_username": telegram_bot_username,
        "telegram_bot_token": telegram_bot_token,
    }

    normalized_chat_id = normalize_telegram_chat_id(telegram_chat_id)
    if not normalized_chat_id:
        return render_telegram_setup_page(
            error="Telegram Chat ID không hợp lệ. Ví dụ: 123456789 hoặc -1001234567890.",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    normalized_username = normalize_telegram_username(telegram_username)
    if telegram_username and not normalized_username:
        return render_telegram_setup_page(
            error="Telegram username không hợp lệ. Ví dụ: nguyenthang_ads hoặc @nguyenthang_ads.",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    normalized_bot_token = normalize_telegram_bot_token(telegram_bot_token)
    if not normalized_bot_token:
        return render_telegram_setup_page(
            error="Bot token chưa sẵn sàng. Hãy tạo bot bằng BotFather rồi dán token của bạn vào đây.",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    resolve_ok, resolved_bot_username, resolve_error = resolve_bot_username_from_token(normalized_bot_token)
    if not resolve_ok:
        return render_telegram_setup_page(
            error=f"Không thể xác thực bot từ token: {resolve_error}",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    normalized_bot_username = normalize_telegram_bot_username(telegram_bot_username)
    if normalized_bot_username and normalized_bot_username != resolved_bot_username:
        return render_telegram_setup_page(
            error="Bot username không khớp với token đã nhập. Hãy kiểm tra lại bot token.",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    telegram_test, _test_message = send_telegram_test_message(
        normalized_chat_id,
        user.get("display_name", pending_username),
        bot_token=normalized_bot_token,
    )

    save_employee_telegram_setup(
        pending_username,
        chat_id=normalized_chat_id,
        telegram_username=normalized_username,
        bot_username=resolved_bot_username,
        bot_token=normalized_bot_token,
        test_status=telegram_test,
    )

    if telegram_test != "sent":
        failed_msg = (
            "Bot Token của bạn chưa hợp lệ. Hãy kiểm tra lại Bot Token rồi thử lại."
            if telegram_test == "not_configured"
            else "Gửi tin test chưa thành công. Hãy bấm Start cho bot rồi thử lại."
        )
        return render_telegram_setup_page(
            error=f"{failed_msg} Nếu chưa có Chat ID, bấm nút 'Tự lấy từ Telegram' để hệ thống tự điền.",
            username=pending_username,
            display_name=user.get("display_name", pending_username),
            form_values=form_values,
            back_url=url_for("register_employee"),
            back_text="Quay lại đăng ký",
            submit_label="Thử gửi test lại",
            title_text="Bước 2: Kết nối Telegram",
            bind_code=ensure_telegram_bind_code(pending_username),
            form_action=url_for("register_telegram"),
        )

    session.pop("pending_telegram_setup", None)
    return redirect(url_for("login", registered="1", telegram_ready="1", telegram_test=telegram_test, username=pending_username))


@app.route("/telegram/connect", methods=["GET", "POST"])
@login_required
def employee_telegram_connect():
    username = session.get("username", "")
    users = load_users_config()
    user = users.get(username)
    if not is_telegram_report_role(user):
        return redirect(url_for("index"))

    next_target = get_safe_next_url(request.args.get("next", ""))
    role = str(user.get("role", "") or "").strip()
    is_privileged = role in {"lead", "admin"}
    back_url = next_target if is_privileged else url_for("logout")
    back_text = "Về dashboard" if is_privileged else "Đăng xuất"
    title_text = "Kết nối Telegram để nhận báo cáo" if is_privileged else "Kết nối Telegram trước khi vào hệ thống"
    form_values = {
        "telegram_chat_id": user.get("telegram_chat_id", ""),
        "telegram_username": user.get("telegram_username", ""),
        "telegram_bot_username": user.get("telegram_bot_username", ""),
        "telegram_bot_token": user.get("telegram_bot_token", ""),
    }

    if request.method == "GET":
        if not user_requires_telegram_setup(username, user):
            return redirect(next_target)
        bind_code = ensure_telegram_bind_code(username)
        return render_telegram_setup_page(
            error="",
            username=username,
            display_name=user.get("display_name", username),
            form_values=form_values,
            back_url=back_url,
            back_text=back_text,
            submit_label="Lưu và gửi test",
            title_text=title_text,
            bind_code=bind_code,
            form_action=url_for("employee_telegram_connect", next=next_target),
        )

    telegram_chat_id = request.form.get("telegram_chat_id", "").strip()
    telegram_username = request.form.get("telegram_username", "").strip()
    telegram_bot_username = request.form.get("telegram_bot_username", "").strip()
    telegram_bot_token = request.form.get("telegram_bot_token", "").strip()
    form_values = {
        "telegram_chat_id": telegram_chat_id,
        "telegram_username": telegram_username,
        "telegram_bot_username": telegram_bot_username,
        "telegram_bot_token": telegram_bot_token,
    }

    normalized_chat_id = normalize_telegram_chat_id(telegram_chat_id)
    if not normalized_chat_id:
        return render_telegram_setup_page(
            error="Telegram Chat ID không hợp lệ. Ví dụ: 123456789 hoặc -1001234567890.",
            username=username,
            display_name=user.get("display_name", username),
            form_values=form_values,
            back_url=back_url,
            back_text=back_text,
            submit_label="Lưu và gửi test",
            title_text=title_text,
            bind_code=ensure_telegram_bind_code(username),
            form_action=url_for("employee_telegram_connect", next=next_target),
        )

    normalized_username = normalize_telegram_username(telegram_username)
    if telegram_username and not normalized_username:
        return render_telegram_setup_page(
            error="Telegram username không hợp lệ. Ví dụ: nguyenthang_ads hoặc @nguyenthang_ads.",
            username=username,
            display_name=user.get("display_name", username),
            form_values=form_values,
            back_url=back_url,
            back_text=back_text,
            submit_label="Lưu và gửi test",
            title_text=title_text,
            bind_code=ensure_telegram_bind_code(username),
            form_action=url_for("employee_telegram_connect", next=next_target),
        )

    resolve_ok, normalized_bot_token, resolved_bot_username, resolve_error = resolve_telegram_setup_bot(
        telegram_bot_token,
        telegram_bot_username,
    )
    if not resolve_ok:
        return render_telegram_setup_page(
            error=f"Không thể xác thực bot từ token: {resolve_error}",
            username=username,
            display_name=user.get("display_name", username),
            form_values=form_values,
            back_url=back_url,
            back_text=back_text,
            submit_label="Lưu và gửi test",
            title_text=title_text,
            bind_code=ensure_telegram_bind_code(username),
            form_action=url_for("employee_telegram_connect", next=next_target),
        )

    telegram_test, _test_message = send_telegram_test_message(
        normalized_chat_id,
        user.get("display_name", username),
        bot_token=normalized_bot_token,
    )

    save_employee_telegram_setup(
        username,
        chat_id=normalized_chat_id,
        telegram_username=normalized_username,
        bot_username=resolved_bot_username,
        bot_token=normalized_bot_token,
        test_status=telegram_test,
    )

    if telegram_test != "sent":
        if is_privileged:
            return redirect(next_target)

        failed_msg = (
            "Bot Token của bạn chưa hợp lệ. Hãy kiểm tra lại Bot Token rồi thử lại."
            if telegram_test == "not_configured"
            else "Gửi tin test chưa thành công. Hãy bấm Start cho bot rồi thử lại."
        )
        return render_telegram_setup_page(
            error=f"{failed_msg} Nếu chưa có Chat ID, bấm nút 'Tự lấy từ Telegram' để hệ thống tự điền.",
            username=username,
            display_name=user.get("display_name", username),
            form_values=form_values,
            back_url=back_url,
            back_text=back_text,
            submit_label="Thử gửi test lại",
            title_text=title_text,
            bind_code=ensure_telegram_bind_code(username),
            form_action=url_for("employee_telegram_connect", next=next_target),
        )

    return redirect(next_target)


@app.route("/api/telegram/autofill", methods=["POST"])
def api_telegram_autofill():
    actor_username, actor_user = get_current_telegram_setup_actor()
    if not actor_username or not actor_user:
        return jsonify({"success": False, "error": "Phiên thiết lập Telegram đã hết hạn. Vui lòng đăng nhập lại."}), 401

    payload = request.get_json(silent=True) or {}
    raw_bot_token = str(payload.get("telegram_bot_token", "")).strip()
    ok, bot_token, bot_username, err = resolve_telegram_setup_bot(raw_bot_token)
    if not ok:
        return jsonify({"success": False, "error": f"Không thể xác thực bot token: {err}"}), 400

    bind_code = ensure_telegram_bind_code(actor_username)
    deep_link = f"https://t.me/{bot_username}?start={bind_code}"
    found, found_payload, found_error = find_chat_from_bind_code(bot_token, bind_code)
    if not found:
        return jsonify({
            "success": True,
            "found": False,
            "bot_username": bot_username,
            "bind_code": bind_code,
            "deep_link": deep_link,
            "hint": "Nhân viên cần mở bot, bấm Start rồi bấm lại nút 'Tự lấy từ Telegram'.",
            "error": found_error,
        })

    return jsonify({
        "success": True,
        "found": True,
        "bot_username": bot_username,
        "bind_code": bind_code,
        "deep_link": deep_link,
        "telegram_chat_id": found_payload.get("chat_id", ""),
        "telegram_username": found_payload.get("telegram_username", ""),
        "message": "Đã tự lấy Chat ID từ Telegram thành công.",
    })


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    if request.args.get("expired", "").strip() == "1":
        return redirect(url_for("login", expired="1"))
    return redirect(url_for("login"))


@app.route("/api/session/ping", methods=["POST"])
@api_login_required
def session_ping():
    return jsonify({"success": True})

@app.route("/api/fetch-data", methods=["POST"])
@api_login_required
def fetch_data():
    data = request.get_json(silent=True) or {}
    sheet_url = (data.get("sheet_url") or "").strip()
    should_sync_meta = bool(data.get("sync_meta", True))
    
    if not sheet_url:
        return jsonify({"success": False, "error": "Vui lòng nhập URL sheet"}), 400
    
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({
            "success": False,
            "error": "URL sheet không hợp lệ.",
            "help_steps": [
                "Mở đúng file Google Sheet cần đọc dữ liệu.",
                "Copy link trên thanh địa chỉ (dạng /spreadsheets/d/... hoặc /spreadsheets/u/1/d/...).",
                "Hoặc dán trực tiếp mã Sheet ID (chuỗi dài phía sau /d/).",
            ],
        }), 400
    
    role = session.get("role", "employee")
    username = session.get("username", "")

    # Access control
    if role == "employee":
        accessible_ids = {extract_sheet_id(s["url"]) for s in get_accessible_sheets_for_user(username) if s.get("url")}
        if sheet_id not in accessible_ids:
            # Allow employee to test a newly entered sheet URL before saving it.
            access = inspect_sheet_access(sheet_url)
            if not access.get("success"):
                return jsonify({
                    "success": False,
                    "error": access.get("error", "Bạn không có quyền xem sheet này."),
                    "help": access.get("help", ""),
                    "help_steps": access.get("help_steps", []),
                    "service_account_email": access.get("service_account_email", ""),
                    "sheet_id": access.get("sheet_id", ""),
                    "clean_url": access.get("clean_url", ""),
                    "share_url": access.get("share_url", ""),
                    "request_access_url": access.get("request_access_url", ""),
                    "can_auto_open_sheet": bool(access.get("can_auto_open_sheet", False)),
                }), 403
    elif role == "lead":
        accessible_ids = {extract_sheet_id(s["url"]) for s in get_accessible_sheets_for_user(username) if s.get("url")}
        if sheet_id not in accessible_ids:
            return jsonify({"success": False, "error": "Sheet này không thuộc team của bạn."}), 403

    sync_meta_result = {"attempted": False, "success": False, "written_rows": 0, "accounts_total": 0, "accounts_synced": 0}
    if should_sync_meta:
        try:
            sync_meta_result = sync_ads_sheet_from_meta(sheet_id, date_preset="today", owner_username=username)
        except Exception as e:
            sync_meta_result = {
                "attempted": True,
                "success": False,
                "written_rows": 0,
                "accounts_total": 0,
                "accounts_synced": 0,
                "hint": f"Không thể tự đồng bộ Meta API: {str(e)}",
            }

    result = fetch_chi_phi_ads_data(sheet_id)
    result["sync_meta"] = sync_meta_result
    return jsonify(result)


@app.route("/api/fetch-all-data", methods=["POST"])
@api_role_required("lead")
def fetch_all_data():
    """Fetch & aggregate data from all sheets accessible to the current user."""
    username = session.get("username", "")
    role = session.get("role", "employee")
    sheets = get_accessible_sheets_for_user(username)

    # Fallback for accounts that can view aggregate reports but have no assigned employee sheet map yet.
    if not sheets and role in {"lead", "admin"}:
        fallback_url = (session.get("sheet_url") or "").strip()
        if fallback_url:
            sheets = [
                {
                    "name": session.get("display_name", username) or username,
                    "url": fallback_url,
                    "team": session.get("team", ""),
                    "username": username,
                }
            ]

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
            if member_rows:
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
                for item in result.get("matrix_member_summary", []) or []:
                    member_summaries.append({
                        "name": item.get("name", ""),
                        "team": item.get("team", "") or sheet.get("team", ""),
                        "total_spend": item.get("total_spend", 0),
                        "total_data": item.get("total_data", 0),
                        "cost_per_data": item.get("cost_per_data", 0),
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


@app.route("/api/account-status", methods=["POST"])
@api_login_required
def account_status():
    data = request.get_json(silent=True) or {}
    sheet_url = (data.get("sheet_url") or "").strip()
    if not sheet_url:
        return jsonify({"success": False, "error": "Thiếu Link chi phí ads."}), 400

    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({"success": False, "error": "Link chi phí ads không hợp lệ."}), 400

    role = session.get("role", "employee")
    username = session.get("username", "")
    if role in {"employee", "lead"}:
        accessible_ids = {extract_sheet_id(s["url"]) for s in get_accessible_sheets_for_user(username) if s.get("url")}
        if sheet_id not in accessible_ids:
            return jsonify({"success": False, "error": "Bạn không có quyền xem trạng thái tài khoản của sheet này."}), 403

    try:
        result = get_sheet_account_statuses(sheet_id)
        return jsonify(result)
    except gspread.exceptions.WorksheetNotFound:
        return jsonify(
            {
                "success": False,
                "error": "Không thấy tab Cài đặt/Settings để lấy danh sách tài khoản quảng cáo.",
                "hint": "Nhân viên chỉ cần tạo tab Cài đặt và điền cột tài khoản quảng cáo như mẫu cũ (cột G).",
                "can_auto_open_sheet": True,
            }
        ), 400
    except Exception as e:
        return jsonify({"success": False, "error": f"Không lấy được trạng thái tài khoản: {str(e)}"}), 500


@app.route("/api/performance-summary", methods=["POST"])
@api_login_required
def performance_summary():
    data = request.get_json(silent=True) or {}
    performance_sheet_url = (data.get("performance_sheet_url") or "").strip()
    if not performance_sheet_url:
        return jsonify({"success": False, "error": "Thiếu Link bảng hiệu suất."}), 400

    sheet_id = extract_sheet_id(performance_sheet_url)
    if not sheet_id:
        return jsonify({"success": False, "error": "Link bảng hiệu suất không hợp lệ."}), 400

    role = session.get("role", "employee")
    username = session.get("username", "")
    if role == "employee":
        allowed_ids = set()
        session_perf_url = (session.get("performance_sheet_url") or "").strip()
        if session_perf_url:
            sid = extract_sheet_id(session_perf_url)
            if sid:
                allowed_ids.add(sid)
        for item in get_user_monthly_sheets(username):
            perf_url = (item.get("performance_sheet_url") or "").strip()
            sid = extract_sheet_id(perf_url)
            if sid:
                allowed_ids.add(sid)
        if allowed_ids and sheet_id not in allowed_ids:
            return jsonify({"success": False, "error": "Bạn không có quyền xem bảng hiệu suất này."}), 403

    try:
        result = fetch_performance_summary(performance_sheet_url)
        return jsonify(result)
    except gspread.exceptions.WorksheetNotFound:
        return jsonify({"success": False, "error": "Không tìm thấy tab dữ liệu trong bảng hiệu suất."}), 400
    except Exception as e:
        raw_error = str(e)
        lower_error = raw_error.lower()
        service_email = get_service_account_client_email()
        access_links = build_sheet_access_links(sheet_id)
        if "permission" in lower_error or "forbidden" in lower_error or "403" in lower_error:
            return jsonify({
                "success": False,
                "error": "Bảng hiệu suất chưa chia sẻ cho service account (Editor/Viewer).",
                "service_account_email": service_email,
                "sheet_id": sheet_id,
                "clean_url": access_links.get("clean_url", ""),
                "share_url": access_links.get("share_url", ""),
                "request_access_url": access_links.get("request_access_url", ""),
                "can_auto_open_sheet": True,
            }), 403
        if "resource_exhausted" in lower_error or "quota exceeded" in lower_error or "rate_limit_exceeded" in lower_error:
            return jsonify({
                "success": False,
                "error": "Google Sheets đang tạm giới hạn số lần đọc dữ liệu (quota/phút). Vui lòng đợi 60-90 giây rồi bấm Tải Dữ Liệu lại.",
            }), 429
        return jsonify({"success": False, "error": f"Không đọc được bảng hiệu suất: {raw_error}"}), 500


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


@app.route("/internal/telegram/reports/run", methods=["POST"])
def run_internal_telegram_reports():
    auth_header = request.headers.get("Authorization", "")
    expected = f"Bearer {INTERNAL_CRON_SECRET}" if INTERNAL_CRON_SECRET else ""
    if not expected or auth_header != expected:
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    result = run_telegram_report_job(force=bool(payload.get("force", False)))
    return jsonify(result)


@app.route("/internal/sheets/daily-reset", methods=["POST"])
def run_internal_daily_sheet_reset():
    """Chạy lúc 00h: xóa bộ lọc tab Tổng, cập nhật AC1:AD1 = ngày hôm nay (USER_ENTERED)."""
    auth_header = request.headers.get("Authorization", "")
    expected = f"Bearer {INTERNAL_CRON_SECRET}" if INTERNAL_CRON_SECRET else ""
    if not expected or auth_header != expected:
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    perf_url = os.environ.get("BUILTIN_ADMIN_PERFORMANCE_SHEET_URL", "")
    if not perf_url:
        return jsonify({"success": False, "error": "Chưa cấu hình BUILTIN_ADMIN_PERFORMANCE_SHEET_URL"}), 400

    try:
        sheet_id = extract_sheet_id(perf_url)
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        tong_ws = resolve_optional_worksheet(spreadsheet, ["TỔNG", "Tong", "Tổng"])
        if tong_ws is None:
            return jsonify({"success": False, "error": "Không tìm thấy tab Tổng"}), 400

        # Bước 1: xóa bộ lọc đang active (nếu có)
        try:
            spreadsheet.batch_update({
                "requests": [{"clearBasicFilter": {"sheetId": tong_ws.id}}]
            })
        except Exception:
            pass  # Không có filter thì bỏ qua

        # Bước 2: ghi ngày hôm nay vào AC1:AD1 với USER_ENTERED
        # để Google Sheets xử lý như người dùng nhập → khớp data validation dropdown
        today_str = datetime.now(tz=ZoneInfo(TELEGRAM_REPORT_TIMEZONE)).strftime("%d/%m/%Y")
        tong_ws.update(
            values=[[today_str, today_str]],
            range_name="AC1:AD1",
            value_input_option="USER_ENTERED",
        )

        return jsonify({"success": True, "date_set": today_str})

    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500



def run_internal_ads_autofill():
    auth_header = request.headers.get("Authorization", "")
    expected = f"Bearer {INTERNAL_CRON_SECRET}" if INTERNAL_CRON_SECRET else ""
    if not expected or auth_header != expected:
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force", False))
    mode = str(payload.get("mode", "today")).strip().lower() or "today"
    if mode not in {"today", "yesterday"}:
        return jsonify({"success": False, "error": "mode chỉ hỗ trợ 'today' hoặc 'yesterday'."}), 400

    if not force and not load_auto_fill_enabled():
        return jsonify({
            "success": True,
            "skipped": True,
            "reason": "Auto Fill đang tắt.",
            "mode": mode,
        })

    result = run_ads_autofill_job(date_preset=mode)
    return jsonify(result)

# ─────────────────── ADMIN USER MANAGEMENT ───────────────────

def save_users_config(config: dict) -> None:
    """Persist user config to the JSON file (used by admin UI)."""
    # Always merge env-var baseline so those accounts survive alongside file-registered ones.
    env_baseline: dict = {}
    config_json = os.getenv("USERS_CONFIG", "").strip()
    if config_json:
        try:
            parsed = json.loads(config_json)
            if isinstance(parsed, dict):
                env_baseline = parsed
        except Exception:
            pass
    merged = dict(env_baseline)
    merged.update(config)  # config (file) wins on conflicts
    merged[BUILTIN_ADMIN_USERNAME] = ensure_builtin_admin_profile(merged.get(BUILTIN_ADMIN_USERNAME, {}))
    _save_users_to_db(merged)
    # Keep local files as a portable backup for non-DB environments.
    atomic_write_json_file(USERS_FILE_PATH, merged)
    atomic_write_json_file(USERS_FILE_BACKUP_PATH, merged)
    # Keep legacy file in sync for backward compatibility with older deployments.
    try:
        atomic_write_json_file(LEGACY_USERS_FILE_PATH, merged)
    except Exception:
        pass


def admin_page_required(view_func):
    """Decorator for admin-only HTML pages (redirect to login/dashboard)."""
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for("login", next=request.path))
        if ROLE_LEVELS.get(session.get("role", ""), 0) < ROLE_LEVELS["admin"]:
            return redirect(url_for("index"))
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


@app.route("/api/debug/performance-weekly", methods=["GET"])
@app.route("/api/debug/lng-headers", methods=["GET"])
@api_role_required("admin")
def api_debug_lng_headers():
    """Debug endpoint: show profitability sheet header row and what product/lng columns were detected."""
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "error": "url param required"})
    try:
        sheet_id = extract_sheet_id(url)
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = resolve_profitability_worksheet(spreadsheet)
        rows = worksheet.get_all_values()
        sample = [list(r) for r in rows[:10]]
        result = build_product_lng_summary(spreadsheet)
        return jsonify({
            "success": True,
            "tab_title": worksheet.title,
            "total_rows": len(rows),
            "first_10_rows": sample,
            "lng_items_count": len(result.get("items", [])),
            "first_3_items": result.get("items", [])[:3],
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)})


@api_role_required("admin")
def api_debug_performance_weekly():
    """Debug endpoint: returns raw rows sample + weekly_trend parse result for a given sheet URL."""
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"success": False, "error": "url param required"})
    try:
        sheet_id = extract_sheet_id(url)
        client = get_gspread_client()
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = resolve_performance_worksheet(spreadsheet)
        rows = worksheet.get_all_values()
        weekly = fetch_performance_weekly_trend(rows)
        # Return first 5 rows as sample to help diagnose
        sample = [list(r)[:10] for r in rows[:10]]
        return jsonify({
            "success": True,
            "tab_title": worksheet.title,
            "total_rows": len(rows),
            "sample_rows": sample,
            "weekly_trend": weekly,
        })
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)})


@app.route("/api/admin/users-config-export", methods=["GET"])
@api_role_required("admin")
def api_admin_export_users_config():
    """Return full users config JSON so admin can copy it into the USERS_CONFIG env var on Render.
    This ensures registered employees survive across deployments."""
    users = load_users_config()
    compact = json.dumps(users, ensure_ascii=False, separators=(",", ":"))
    return jsonify({"success": True, "users_config_json": compact, "count": len(users)})


@app.route("/api/admin/users", methods=["GET"])
@api_role_required("admin")
def api_admin_list_users():
    users = load_users_config()
    result = []
    for uname, udata in users.items():
        telegram_chat_id = udata.get("telegram_chat_id", "")
        telegram_username = udata.get("telegram_username", "")
        telegram_bot_username = udata.get("telegram_bot_username", "")
        result.append({
            "username": uname,
            "role": udata.get("role", "employee"),
            "team": udata.get("team", ""),
            "display_name": udata.get("display_name", uname),
            "sheet_url": udata.get("sheet_url", ""),
            "telegram_chat_id": telegram_chat_id,
            "telegram_username": telegram_username,
            "telegram_bot_username": telegram_bot_username,
            "telegram_connected": bool(telegram_chat_id),
            "telegram_verified": bool(udata.get("telegram_verified", False)),
            "telegram_test_status": udata.get("telegram_test_status", ""),
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
        users[username]["telegram_verified"] = False
        users[username]["telegram_test_status"] = "pending"
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
    sheet_url = (data.get("sheet_url") or "").strip()
    performance_sheet_url = (data.get("performance_sheet_url") or "").strip()
    performance_sheet_name = ""
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return jsonify({
            "success": False,
            "error": "URL sheet không hợp lệ.",
            "help_steps": [
                "Mở đúng file Google Sheet cần đọc dữ liệu.",
                "Copy link trên thanh địa chỉ (dạng /spreadsheets/d/... hoặc /spreadsheets/u/1/d/...).",
                "Hoặc dán trực tiếp mã Sheet ID (chuỗi dài phía sau /d/).",
            ],
        }), 400

    if performance_sheet_url and not extract_sheet_id(performance_sheet_url):
        return jsonify({
            "success": False,
            "error": "URL Link bảng hiệu suất không hợp lệ.",
            "help_steps": [
                "Mở đúng file Google Sheet hiệu suất cần dùng.",
                "Copy link trên thanh địa chỉ (dạng /spreadsheets/d/... hoặc /spreadsheets/u/1/d/...).",
                "Hoặc dán trực tiếp mã Sheet ID (chuỗi dài phía sau /d/).",
            ],
        }), 400
    if performance_sheet_url:
        perf_name, _, perf_clean_url = get_sheet_name_and_month(performance_sheet_url)
        performance_sheet_name = perf_name or ""
        performance_sheet_url = perf_clean_url or performance_sheet_url

    username = session.get("username", "")
    role = session.get("role", "employee")
    access = inspect_sheet_access(sheet_url)
    if not access.get("success"):
        return jsonify({
            "success": False,
            "error": access.get("error", "Không thể kết nối Google Sheet."),
            "help": access.get("help", ""),
            "help_steps": access.get("help_steps", []),
            "service_account_email": access.get("service_account_email", ""),
            "sheet_id": access.get("sheet_id", ""),
            "clean_url": access.get("clean_url", ""),
            "share_url": access.get("share_url", ""),
            "request_access_url": access.get("request_access_url", ""),
            "can_auto_open_sheet": bool(access.get("can_auto_open_sheet", False)),
        }), 400

    sheet_name = access.get("sheet_name", "")
    month_key = access.get("month_key", current_month_key())
    clean_url = access.get("clean_url", "")
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
        save_monthly_sheet_record(
            username,
            clean_url,
            sheet_name,
            month_key,
            performance_sheet_url=performance_sheet_url,
            performance_sheet_name=performance_sheet_name,
        )

        if role == "employee":
            users = load_users_config()
            if username in users:
                users[username]["sheet_url"] = clean_url
                users[username]["performance_sheet_url"] = performance_sheet_url
                save_users_config(users)
            session["sheet_url"] = clean_url
            session["performance_sheet_url"] = performance_sheet_url

    msg = f'Đã lưu sheet "{sheet_name}" vào thư mục tháng {month_label(month_key)}.'
    return jsonify({
        "success": True,
        "message": msg,
        "name": sheet_name,
        "already_exists": already_exists,
        "month_key": month_key,
        "month_label": month_label(month_key),
        "clean_url": clean_url,
        "service_account_email": access.get("service_account_email", ""),
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

def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Chi Phi Ads web app utility commands")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reports_parser = subparsers.add_parser(
        "send-telegram-reports",
        help="Build and send realtime Telegram report messages for employees",
    )
    reports_parser.add_argument("--username", action="append", help="Chi gui cho 1 user cu the. Co the lap lai flag nay.")
    reports_parser.add_argument("--dry-run", action="store_true", help="In noi dung tin nhan ra terminal, khong gui that")
    reports_parser.add_argument("--force", action="store_true", help="Bo qua gio gui va check duplicate slot")

    trigger_parser = subparsers.add_parser(
        "trigger-telegram-reports",
        help="Call the internal cron endpoint to run Telegram reports on the live web service",
    )
    trigger_parser.add_argument("--force", action="store_true", help="Gui kem force=true cho internal cron endpoint")
    return parser


def main_cli() -> int:
    parser = build_cli_parser()
    args = parser.parse_args()

    if args.command == "send-telegram-reports":
        result = run_telegram_report_job(
            force=bool(args.force),
            dry_run=bool(args.dry_run),
            usernames=args.username,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if args.command == "trigger-telegram-reports":
        return trigger_telegram_report_job_command(force=bool(args.force))

    parser.print_help()
    return 1


# ─────────────────── CHANGE PASSWORD ───────────────────

def _clear_pw_otp_session() -> None:
    for k in ["pw_otp_code", "pw_otp_expires", "pw_otp_new_password", "pw_otp_attempts", "pw_otp_step"]:
        session.pop(k, None)


def _clear_forgot_pw_otp_session() -> None:
    for k in [
        "fp_otp_code",
        "fp_otp_expires",
        "fp_otp_new_password",
        "fp_otp_attempts",
        "fp_otp_step",
        "fp_otp_username",
    ]:
        session.pop(k, None)


def _validate_new_password(new_password: str, confirm_password: str, current_password: str = "") -> str:
    if not new_password or len(new_password) < 6:
        return "Mật khẩu mới phải có ít nhất 6 ký tự."
    if new_password != confirm_password:
        return "Mật khẩu xác nhận không khớp."
    if current_password and new_password == current_password:
        return "Mật khẩu mới phải khác mật khẩu hiện tại."
    return ""


def _send_password_otp_message(user: dict, username: str, otp_code: str, title_text: str) -> tuple[bool, str]:
    chat_id = normalize_telegram_chat_id(str(user.get("telegram_chat_id", "")))
    if not chat_id:
        return False, "Tài khoản chưa liên kết Telegram. Vui lòng liên hệ admin để reset mật khẩu."

    bot_token = normalize_telegram_bot_token(str(user.get("telegram_bot_token", ""))) or TELEGRAM_BOT_TOKEN
    display = html.escape(user.get("display_name", username))
    msg_text = (
        f"🔐 <b>{title_text}</b>\n\n"
        f"Xin chào <b>{display}</b>,\n\n"
        f"Mã xác nhận của bạn là:\n\n"
        f"<b>🔢 {otp_code}</b>\n\n"
        f"Mã có hiệu lực trong <b>5 phút</b>.\n"
        f"Nếu bạn không yêu cầu thao tác này, hãy bỏ qua tin nhắn này."
    )
    return send_telegram_message(chat_id, msg_text, bot_token)


@app.route("/change-password", methods=["GET"])
@login_required
def change_password_page():
    step = session.get("pw_otp_step", "form")
    return render_template("change_password.html", step=step, error="", success="")


@app.route("/change-password/request", methods=["POST"])
@login_required
def change_password_request():
    username = session.get("username", "")
    users = load_users_config()
    user = users.get(username)

    if not user:
        return render_template("change_password.html", step="form",
                               error="Không tìm thấy tài khoản.", success="")

    old_password = request.form.get("old_password", "")
    new_password = request.form.get("new_password", "")
    confirm_password = request.form.get("confirm_password", "")

    if user.get("password") != old_password:
        return render_template("change_password.html", step="form",
                               error="Mật khẩu hiện tại không đúng.", success="")

    if not new_password or len(new_password) < 6:
        return render_template("change_password.html", step="form",
                               error="Mật khẩu mới phải có ít nhất 6 ký tự.", success="")

    if new_password != confirm_password:
        return render_template("change_password.html", step="form",
                               error="Mật khẩu xác nhận không khớp.", success="")

    if new_password == old_password:
        return render_template("change_password.html", step="form",
                               error="Mật khẩu mới phải khác mật khẩu hiện tại.", success="")

    chat_id = normalize_telegram_chat_id(str(user.get("telegram_chat_id", "")))
    if not chat_id:
        return render_template("change_password.html", step="form",
                               error="Tài khoản chưa liên kết Telegram. Vui lòng liên hệ admin để reset mật khẩu.",
                               success="")

    otp_code = str(secrets.randbelow(10000)).zfill(4)
    session["pw_otp_code"] = otp_code
    session["pw_otp_expires"] = (datetime.now() + timedelta(minutes=5)).timestamp()
    session["pw_otp_new_password"] = new_password
    session["pw_otp_attempts"] = 0
    session["pw_otp_step"] = "otp"

    bot_token = normalize_telegram_bot_token(str(user.get("telegram_bot_token", ""))) or TELEGRAM_BOT_TOKEN
    display = html.escape(user.get("display_name", username))
    msg_text = (
        f"🔐 <b>Xác nhận đổi mật khẩu</b>\n\n"
        f"Xin chào <b>{display}</b>,\n\n"
        f"Mã xác nhận đổi mật khẩu của bạn là:\n\n"
        f"<b>🔢 {otp_code}</b>\n\n"
        f"Mã có hiệu lực trong <b>5 phút</b>.\n"
        f"Nếu bạn không yêu cầu đổi mật khẩu, hãy bỏ qua tin nhắn này."
    )
    ok, err = send_telegram_message(chat_id, msg_text, bot_token)
    if not ok:
        _clear_pw_otp_session()
        return render_template("change_password.html", step="form",
                               error=f"Không thể gửi mã xác nhận qua Telegram: {err}", success="")

    return render_template("change_password.html", step="otp", error="", success="")


@app.route("/change-password/verify", methods=["POST"])
@login_required
def change_password_verify():
    entered_code = request.form.get("otp_code", "").strip()
    stored_code = str(session.get("pw_otp_code", ""))
    expires = float(session.get("pw_otp_expires", 0))
    new_password = str(session.get("pw_otp_new_password", ""))
    attempts = int(session.get("pw_otp_attempts", 0))

    if not stored_code or not new_password:
        _clear_pw_otp_session()
        return render_template("change_password.html", step="form",
                               error="Phiên đổi mật khẩu đã hết hạn. Vui lòng thử lại.", success="")

    if datetime.now().timestamp() > expires:
        _clear_pw_otp_session()
        return render_template("change_password.html", step="form",
                               error="Mã xác nhận đã hết hạn (5 phút). Vui lòng thử lại.", success="")

    if attempts >= 3:
        _clear_pw_otp_session()
        return render_template("change_password.html", step="form",
                               error="Đã nhập sai quá 3 lần. Vui lòng yêu cầu mã mới.", success="")

    if not secrets.compare_digest(entered_code, stored_code):
        session["pw_otp_attempts"] = attempts + 1
        remaining = 3 - (attempts + 1)
        return render_template("change_password.html", step="otp",
                               error=f"Mã xác nhận không đúng. Còn {remaining} lần thử.", success="")

    username = session.get("username", "")
    users = load_users_config()
    if username not in users:
        _clear_pw_otp_session()
        return render_template("change_password.html", step="form",
                               error="Không tìm thấy tài khoản.", success="")

    users[username]["password"] = new_password
    save_users_config(users)
    _clear_pw_otp_session()

    return render_template("change_password.html", step="done", error="",
                           success="Mật khẩu đã được đổi thành công!")


@app.route("/forgot-password", methods=["GET"])
def forgot_password_page():
    step = session.get("fp_otp_step", "form")
    return render_template("forgot_password.html", step=step, error="", success="")


@app.route("/forgot-password/request", methods=["POST"])
def forgot_password_request():
    username = normalize_username(request.form.get("username", ""))
    new_password = request.form.get("new_password", "")
    confirm_password = request.form.get("confirm_password", "")

    if not username:
        return render_template("forgot_password.html", step="form", error="Vui lòng nhập tên đăng nhập.", success="")

    users = load_users_config()
    username_key, user = get_user_entry(username, users)
    if not user:
        return render_template("forgot_password.html", step="form", error="Tài khoản chưa tồn tại trong hệ thống.", success="")

    password_error = _validate_new_password(new_password, confirm_password, str(user.get("password", "")))
    if password_error:
        return render_template("forgot_password.html", step="form", error=password_error, success="")

    otp_code = str(secrets.randbelow(10000)).zfill(4)
    session["fp_otp_code"] = otp_code
    session["fp_otp_expires"] = (datetime.now() + timedelta(minutes=5)).timestamp()
    session["fp_otp_new_password"] = new_password
    session["fp_otp_attempts"] = 0
    session["fp_otp_username"] = username_key or username
    session["fp_otp_step"] = "otp"

    ok, err = _send_password_otp_message(user, username_key or username, otp_code, "Xác nhận quên mật khẩu")
    if not ok:
        _clear_forgot_pw_otp_session()
        return render_template("forgot_password.html", step="form", error=f"Không thể gửi mã xác nhận qua Telegram: {err}", success="")

    return render_template("forgot_password.html", step="otp", error="", success="")


@app.route("/forgot-password/verify", methods=["POST"])
def forgot_password_verify():
    entered_code = request.form.get("otp_code", "").strip()
    stored_code = str(session.get("fp_otp_code", ""))
    expires = float(session.get("fp_otp_expires", 0))
    new_password = str(session.get("fp_otp_new_password", ""))
    username = str(session.get("fp_otp_username", ""))
    attempts = int(session.get("fp_otp_attempts", 0))

    if not stored_code or not new_password or not username:
        _clear_forgot_pw_otp_session()
        return render_template("forgot_password.html", step="form", error="Phiên quên mật khẩu đã hết hạn. Vui lòng thử lại.", success="")

    if datetime.now().timestamp() > expires:
        _clear_forgot_pw_otp_session()
        return render_template("forgot_password.html", step="form", error="Mã xác nhận đã hết hạn (5 phút). Vui lòng thử lại.", success="")

    if attempts >= 3:
        _clear_forgot_pw_otp_session()
        return render_template("forgot_password.html", step="form", error="Đã nhập sai quá 3 lần. Vui lòng yêu cầu mã mới.", success="")

    if not secrets.compare_digest(entered_code, stored_code):
        session["fp_otp_attempts"] = attempts + 1
        remaining = 3 - (attempts + 1)
        return render_template("forgot_password.html", step="otp", error=f"Mã xác nhận không đúng. Còn {remaining} lần thử.", success="")

    users = load_users_config()
    if username not in users:
        _clear_forgot_pw_otp_session()
        return render_template("forgot_password.html", step="form", error="Không tìm thấy tài khoản.", success="")

    users[username]["password"] = new_password
    save_users_config(users)
    _clear_forgot_pw_otp_session()

    return render_template("forgot_password.html", step="done", error="", success="Đặt lại mật khẩu thành công!")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        raise SystemExit(main_cli())

    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "1") == "1"
    app.run(debug=debug, host="0.0.0.0", port=port)
