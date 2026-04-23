import argparse
import contextlib
import getpass
import io
import json
import msvcrt
import time
from datetime import datetime
from pathlib import Path

import requests

from app import fill_all_sheets_command

BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
CREDS_DIR = STORAGE_DIR / "credentials"
TOKEN_PATH = CREDS_DIR / "meta_access_token.json"
META_GRAPH_VERSION = "v20.0"
LOG_DIR = STORAGE_DIR / "logs"
AUTO_LOG_PATH = LOG_DIR / "auto_fill.log"
AUTO_LOCK_PATH = LOG_DIR / "auto_fill.lock"
AUTO_STATE_PATH = STORAGE_DIR / "config" / "auto_fill_state.json"
_daemon_lock_handle = None


def ensure_dirs() -> None:
    CREDS_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    AUTO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)


def append_log(message: str) -> None:
    ensure_dirs()
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with AUTO_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(f"[{stamp}] {message}\n")


def load_auto_fill_enabled() -> bool:
    """Trang thai mac dinh: bat (True) neu chua co cau hinh."""
    ensure_dirs()
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


def acquire_daemon_lock() -> bool:
    global _daemon_lock_handle
    ensure_dirs()

    handle = AUTO_LOCK_PATH.open("a+")
    try:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        handle.close()
        return False

    _daemon_lock_handle = handle
    return True


def run_once_with_capture(mode: str, sheet_urls_file: str = "") -> int:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        code = run_once(mode=mode, sheet_urls_file=sheet_urls_file)

    captured = buf.getvalue().strip()
    if captured:
        append_log(captured)
    append_log(f"Run mode={mode} finished with code={code}")
    return code


def save_token(token: str) -> None:
    ensure_dirs()
    payload = {
        "access_token": token.strip(),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with TOKEN_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_token() -> str:
    if not TOKEN_PATH.exists():
        raise FileNotFoundError(
            "Chua co token. Chay: python fb_ads_tool.py set-token --token <META_TOKEN>"
        )

    with TOKEN_PATH.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    token = (payload.get("access_token") or "").strip()
    if not token:
        raise ValueError("Token trong file dang rong. Hay set-token lai.")
    return token


def run_once(mode: str, sheet_urls_file: str = "") -> int:
    token = load_token()
    return fill_all_sheets_command(sheet_urls_file or None, token, mode)


def validate_token(account_id: str) -> int:
    token = load_token()
    normalized_id = account_id.strip().removeprefix("act_")
    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{normalized_id}/insights"
    params = {
        "fields": "spend",
        "date_preset": "today",
        "limit": 1,
        "access_token": token,
    }

    response = requests.get(endpoint, params=params, timeout=20)
    if response.ok:
        payload = response.json()
        print("Token hop le cho account nay.")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print("Token khong dung duoc cho account nay.")
    print(f"status_code: {response.status_code}")
    try:
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    except Exception:
        print(response.text)
    return 1


def run_daemon(sheet_urls_file: str = "") -> int:
    if not acquire_daemon_lock():
        append_log("Phat hien daemon dang chay, bo qua lan khoi dong trung.")
        return 0

    append_log("Bat dau auto mode")
    append_log("- 07:00: chot du lieu hom qua (mode=yesterday)")
    append_log("- 06:00-23:59: cap nhat realtime moi 15 phut (mode=today)")

    last_yesterday_run = ""
    done_realtime_slots = set()
    last_enabled_state = None

    try:
        while True:
            now = datetime.now()
            day_key = now.strftime("%Y-%m-%d")
            hm = now.strftime("%H:%M")
            enabled = load_auto_fill_enabled()

            if enabled != last_enabled_state:
                append_log("Auto-fill state: ON" if enabled else "Auto-fill state: OFF")
                last_enabled_state = enabled

            if not enabled:
                done_realtime_slots = {x for x in done_realtime_slots if x.startswith(day_key)}
                time.sleep(20)
                continue

            # Job 7h sang: ghi du lieu hom qua, moi ngay 1 lan.
            if hm == "07:00" and last_yesterday_run != day_key:
                append_log("Chay mode=yesterday")
                code = run_once_with_capture(mode="yesterday", sheet_urls_file=sheet_urls_file)
                if code == 0:
                    last_yesterday_run = day_key

            # Realtime: tu 06:00 den 23:59, moi 15 phut.
            if 6 <= now.hour <= 23 and now.minute % 15 == 0:
                slot_key = f"{day_key}_{hm}"
                if slot_key not in done_realtime_slots:
                    append_log("Chay mode=today")
                    run_once_with_capture(mode="today", sheet_urls_file=sheet_urls_file)
                    done_realtime_slots.add(slot_key)

            # Don bo nho slot cu.
            done_realtime_slots = {x for x in done_realtime_slots if x.startswith(day_key)}
            time.sleep(20)

    except KeyboardInterrupt:
        append_log("Da dung auto mode.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tool tu dong fill chi phi vao tab 'Chi phi ADS' cho cac sheet da khai bao"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    set_token_parser = subparsers.add_parser("set-token", help="Luu Meta access token vao local")
    set_token_parser.add_argument("--token", required=True, help="Meta access token")

    subparsers.add_parser(
        "set-token-local",
        help="Nhap Meta access token truc tiep tren may nay, khong can dan vao chat",
    )

    run_once_parser = subparsers.add_parser("run-once", help="Chay 1 lan fill sheet")
    run_once_parser.add_argument(
        "--mode",
        choices=["today", "yesterday"],
        default="today",
        help="today: realtime, yesterday: chot du lieu hom qua",
    )
    run_once_parser.add_argument(
        "--sheet-urls-file",
        default="",
        help="CSV chua cot sheet_url. Mac dinh dung storage/input/sheet_urls.csv",
    )

    daemon_parser = subparsers.add_parser("run-auto", help="Chay tu dong theo lich")
    daemon_parser.add_argument(
        "--sheet-urls-file",
        default="",
        help="CSV chua cot sheet_url. Mac dinh dung storage/input/sheet_urls.csv",
    )

    validate_parser = subparsers.add_parser(
        "validate-token",
        help="Kiem tra token hien tai co doc duoc insights cua 1 account hay khong",
    )
    validate_parser.add_argument("--account-id", required=True, help="ID tai khoan quang cao de test")

    return parser


def main() -> int:
    ensure_dirs()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "set-token":
        save_token(args.token)
        print(f"Da luu token vao: {TOKEN_PATH}")
        return 0

    if args.command == "set-token-local":
        token = getpass.getpass("Nhap Meta access token: ").strip()
        if not token:
            print("Token rong, khong luu.")
            return 1
        save_token(token)
        print(f"Da luu token vao: {TOKEN_PATH}")
        return 0

    if args.command == "run-once":
        return run_once(mode=args.mode, sheet_urls_file=args.sheet_urls_file)

    if args.command == "run-auto":
        return run_daemon(sheet_urls_file=args.sheet_urls_file)

    if args.command == "validate-token":
        return validate_token(args.account_id)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
