import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
INPUT_DIR = STORAGE_DIR / "input"
OUTPUT_DIR = STORAGE_DIR / "output"
LOG_DIR = STORAGE_DIR / "logs"
CREDS_DIR = STORAGE_DIR / "credentials"
CONFIG_PATH = BASE_DIR / "config" / "schedule.json"
OWNER_MAP_PATH = INPUT_DIR / "account_owner.csv"
OUTPUT_PATH = OUTPUT_DIR / "daily_ads_cost.csv"
META_GRAPH_VERSION = "v20.0"
GOOGLE_SHEETS_CSV_TEMPLATE = (
    "https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
)
SETTINGS_SHEET_NAME = "Cài đặt"
CHI_PHI_ADS_SHEET = "Chi phí ADS"
SHEET_URLS_PATH = INPUT_DIR / "sheet_urls.csv"
SERVICE_ACCOUNT_PATH = CREDS_DIR / "service_account.json"
SHEET_DATA_START_ROW = 3  # Hàng 1=header, hàng 2=tổng/filter, hàng 3 trở đi là data
# Neu gap qua nhieu dong trong lien tiep sau khi da co data, xem nhu ket thuc vung data logic.
MAX_EMPTY_STREAK_TO_STOP_SCAN = 120


@dataclass
class OwnerMapping:
    account_id: str
    owner_name: str


@dataclass
class SheetInspectionResult:
    owner_name: str
    account_ids: List[str]
    sheet_id: str
    sheet_url: str


@dataclass
class AccountSpendResult:
    account_id: str
    spend: float


@dataclass
class AccountCheckResult:
    owner_name: str
    sheet_id: str
    sheet_url: str
    account_id: str
    status: str
    spend_today: Optional[float]
    error: str


def ensure_runtime_directories() -> None:
    for path in (INPUT_DIR, OUTPUT_DIR, LOG_DIR, CREDS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def normalize_account_id(raw_id: str) -> str:
    value = (raw_id or "").strip()
    if value.startswith("act_"):
        value = value[4:]
    return value


def extract_sheet_id(sheet_url: str) -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError("Khong doc duoc sheet_id tu URL Google Sheet.")
    return match.group(1)


def get_sheet_title(sheet_url: str) -> str:
    response = requests.get(sheet_url, timeout=20)
    response.raise_for_status()

    match = re.search(r"<title>(.*?)</title>", response.text, re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError("Khong doc duoc tieu de Google Sheet.")

    title = match.group(1).strip()
    suffix = " - Google Trang tính"
    if title.endswith(suffix):
        title = title[: -len(suffix)]
    return title.strip()


def extract_owner_name_from_title(title: str) -> str:
    parts = [part.strip() for part in title.split("-") if part.strip()]
    if not parts:
        raise ValueError("Khong tach duoc ten nhan su tu tieu de sheet.")
    return parts[-1]


def fetch_sheet_csv_rows(sheet_id: str, sheet_name: str) -> List[List[str]]:
    url = GOOGLE_SHEETS_CSV_TEMPLATE.format(sheet_id=sheet_id, sheet_name=sheet_name)
    response = requests.get(url, timeout=20)
    response.raise_for_status()

    text = response.text.lstrip("\ufeff")
    return list(csv.reader(text.splitlines()))


def extract_account_ids_from_settings_rows(rows: List[List[str]]) -> List[str]:
    account_ids: List[str] = []
    seen = set()

    for row in rows:
        candidate = row[6].strip() if len(row) > 6 else ""
        if not candidate:
            continue

        match = re.search(r"\((\d+)\)", candidate)
        if not match:
            continue

        account_id = normalize_account_id(match.group(1))
        if account_id and account_id not in seen:
            seen.add(account_id)
            account_ids.append(account_id)

    return account_ids


def inspect_sheet(sheet_url: str) -> SheetInspectionResult:
    parsed = urlparse(sheet_url)
    if "docs.google.com" not in parsed.netloc:
        raise ValueError("URL khong phai Google Sheet hop le.")

    sheet_id = extract_sheet_id(sheet_url)
    title = get_sheet_title(sheet_url)
    owner_name = extract_owner_name_from_title(title)
    rows = fetch_sheet_csv_rows(sheet_id=sheet_id, sheet_name=SETTINGS_SHEET_NAME)
    account_ids = extract_account_ids_from_settings_rows(rows)

    if not account_ids:
        raise ValueError("Khong tim thay ID tkqc nao trong cot G cua sheet Cai dat.")

    return SheetInspectionResult(
        owner_name=owner_name,
        account_ids=account_ids,
        sheet_id=sheet_id,
        sheet_url=sheet_url,
    )


def load_schedule_slots() -> List[str]:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Khong tim thay file cau hinh: {CONFIG_PATH}")

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    slots = data.get("time_slots", [])
    if not slots:
        raise ValueError("time_slots dang rong trong config/schedule.json")

    for slot in slots:
        datetime.strptime(slot, "%H:%M")

    return sorted(slots)


def load_owner_map() -> Dict[str, OwnerMapping]:
    if not OWNER_MAP_PATH.exists():
        raise FileNotFoundError(f"Khong tim thay bang mapping: {OWNER_MAP_PATH}")

    mapping: Dict[str, OwnerMapping] = {}
    with OWNER_MAP_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required_fields = {"account_id", "owner_name"}
        if not required_fields.issubset(set(reader.fieldnames or [])):
            raise ValueError("account_owner.csv can co 2 cot: account_id, owner_name")

        for row in reader:
            account_id = normalize_account_id(row.get("account_id", ""))
            owner_name = (row.get("owner_name", "") or "").strip()
            if account_id and owner_name:
                mapping[account_id] = OwnerMapping(account_id=account_id, owner_name=owner_name)

    return mapping


def write_owner_map_rows(rows: List[Dict[str, str]]) -> None:
    fieldnames = ["account_id", "owner_name"]
    OWNER_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OWNER_MAP_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upsert_owner_mappings(owner_name: str, account_ids: List[str]) -> None:
    rows = read_csv_rows(OWNER_MAP_PATH)
    normalized_ids = [normalize_account_id(account_id) for account_id in account_ids]
    remaining_rows = [
        row for row in rows if normalize_account_id(row.get("account_id", "")) not in normalized_ids
    ]

    for account_id in normalized_ids:
        remaining_rows.append({"account_id": account_id, "owner_name": owner_name})

    remaining_rows.sort(key=lambda row: (row.get("owner_name", ""), row.get("account_id", "")))
    write_owner_map_rows(remaining_rows)


def resolve_owner(account_id: str, mapping: Dict[str, OwnerMapping]) -> Optional[OwnerMapping]:
    return mapping.get(normalize_account_id(account_id))


def get_current_slot(now: datetime, slots: List[str]) -> str:
    hm = now.strftime("%H:%M")
    if hm in slots:
        return hm

    now_minutes = now.hour * 60 + now.minute
    slot_minutes: List[Tuple[int, str]] = []
    for slot in slots:
        h, m = [int(x) for x in slot.split(":")]
        slot_minutes.append((h * 60 + m, slot))

    valid = [item for item in slot_minutes if item[0] <= now_minutes]
    if valid:
        return valid[-1][1]

    return slots[0]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    fieldnames = ["date", "time_slot", "account_id", "owner_name", "spend"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_spend_staging_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "date",
        "fetched_at",
        "sheet_id",
        "sheet_url",
        "owner_name",
        "account_id",
        "spend",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_full_check_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "owner_name",
        "sheet_id",
        "sheet_url",
        "account_id",
        "status",
        "spend_today",
        "error",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_ready_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    fieldnames = [
        "date",
        "owner_name",
        "sheet_id",
        "sheet_url",
        "account_id",
        "spend",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upsert_daily_cost(
    date_str: str,
    time_slot: str,
    account_id: str,
    owner_name: str,
    spend: float,
) -> None:
    rows = read_csv_rows(OUTPUT_PATH)
    account_id = normalize_account_id(account_id)

    updated = False
    for row in rows:
        if (
            row.get("date") == date_str
            and row.get("time_slot") == time_slot
            and normalize_account_id(row.get("account_id", "")) == account_id
        ):
            row["owner_name"] = owner_name
            row["spend"] = f"{spend:.2f}"
            updated = True
            break

    if not updated:
        rows.append(
            {
                "date": date_str,
                "time_slot": time_slot,
                "account_id": account_id,
                "owner_name": owner_name,
                "spend": f"{spend:.2f}",
            }
        )

    rows.sort(key=lambda x: (x["date"], x["time_slot"], x["account_id"]))
    write_csv_rows(OUTPUT_PATH, rows)


def get_gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials

    if not SERVICE_ACCOUNT_PATH.exists():
        raise FileNotFoundError(f"Khong tim thay file service account: {SERVICE_ACCOUNT_PATH}")

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_PATH), scopes=scopes)
    return gspread.authorize(creds)


def get_account_name_from_meta(account_id: str, access_token: str) -> str:
    account_id = normalize_account_id(account_id)
    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{account_id}"
    params = {"fields": "name", "access_token": access_token}
    response = requests.get(endpoint, params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("name", account_id)


def fetch_campaign_insights(account_id: str, access_token: str, date_preset: str) -> List[dict]:
    account_id = normalize_account_id(account_id)
    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{account_id}/insights"
    params = {
        "fields": "campaign_name,spend,actions",
        "level": "campaign",
        "date_preset": date_preset,
        "access_token": access_token,
        "limit": 100,
    }
    response = requests.get(endpoint, params=params, timeout=20)
    response.raise_for_status()
    return response.json().get("data", [])


def extract_product_name(campaign_name: str) -> str:
    match = re.search(r"BID\s*1_(.+?)(?:_|$)", campaign_name, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return campaign_name.strip()


def aggregate_sheet_rows(rows_to_write: List[dict]) -> List[dict]:
    grouped: Dict[Tuple[str, str, str], dict] = {}

    for row in rows_to_write:
        account_id = normalize_account_id(str(row.get("account_id", "")).strip())
        account_key = account_id if account_id else row["account_name"]
        key = (row["date_vn"], account_key, row["product_name"])
        if key not in grouped:
            grouped[key] = {
                "date_vn": row["date_vn"],
                "account_id": account_id,
                "account_name": row["account_name"],
                "product_name": row["product_name"],
                "data_count": int(row["data_count"]),
                "spend": float(row["spend"]),
            }
            continue

        grouped[key]["data_count"] += int(row["data_count"])
        grouped[key]["spend"] += float(row["spend"])

    return list(grouped.values())


def sum_actions(actions: list) -> int:
    """Lay offsite_conversion.fb_pixel_complete_registration (Results metric)."""
    total = 0
    for action in (actions or []):
        action_type = action.get("action_type", "")
        if action_type == 'offsite_conversion.fb_pixel_complete_registration':
            try:
                total += int(float(action.get("value", 0)))
            except (ValueError, TypeError):
                pass
    return total


def format_date_vn(date_str: str) -> str:
    """Chuyen YYYY-MM-DD sang DD/MM/YYYY cho khop voi format sheet."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%d/%m/%Y")


def extract_account_id_from_label(label: str) -> str:
    match = re.search(r"\((\d{6,})\)", label or "")
    return match.group(1) if match else ""


def resolve_account_name_for_sheet(all_values: List[List[str]], account_id: str, account_name_raw: str) -> str:
    """Uu tien dung dung gia tri da co san trong cot B (data validation)."""
    account_id = normalize_account_id(account_id)
    for i, row in enumerate(all_values):
        if i < SHEET_DATA_START_ROW - 1:
            continue
        cell_account = row[1].strip() if len(row) > 1 else ""
        if not cell_account:
            continue
        if f"({account_id})" in cell_account:
            return cell_account

    if account_id and f"({account_id})" not in account_name_raw:
        return f"{account_name_raw} ({account_id})"
    return account_name_raw


def has_core_data_in_chi_phi_ads_row(row: List[str]) -> bool:
    """Data hop le o tab Chi phi ADS: cot A co ngay va co thong tin tai khoan/ten san pham."""
    cell_date = row[0].strip() if len(row) > 0 else ""
    cell_account = row[1].strip() if len(row) > 1 else ""
    cell_product = row[3].strip() if len(row) > 3 else ""
    return bool(cell_date and (cell_account or cell_product))


def detect_logical_last_data_row(all_values: List[List[str]]) -> int:
    """
    Tim dong cuoi cung cua cum data logic, bo qua cac vung o xa bi formatting/rac.
    Co che: sau khi da thay data, neu gap N dong trong lien tiep thi dung scan.
    """
    last_data_row = SHEET_DATA_START_ROW - 1
    seen_data = False
    empty_streak = 0

    for i in range(SHEET_DATA_START_ROW - 1, len(all_values)):
        row = all_values[i]
        if has_core_data_in_chi_phi_ads_row(row):
            seen_data = True
            empty_streak = 0
            last_data_row = i + 1
            continue

        if seen_data:
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_STREAK_TO_STOP_SCAN:
                break

    return last_data_row


def upsert_rows_to_chi_phi_ads(sheet_id: str, rows_to_write: List[dict]) -> int:
    """
    rows_to_write: list of {date_vn, account_id, account_name, product_name, data_count, spend}
    Cot: A=Ngay, B=Ten TK, C=Loai TK, D=Ten san pham, E=So Data, F=Chi phi VND
    Tra ve so dong da ghi.
    """
    rows_to_write = aggregate_sheet_rows(rows_to_write)

    client = get_gspread_client()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.worksheet(CHI_PHI_ADS_SHEET)

    all_values = worksheet.get_all_values()
    logical_last_data_row = detect_logical_last_data_row(all_values)
    written = 0

    for row_data in rows_to_write:
        target_date = row_data["date_vn"]
        account_id = normalize_account_id(str(row_data.get("account_id", "")).strip())
        account_name_raw = row_data["account_name"]
        account_name = resolve_account_name_for_sheet(all_values, account_id, account_name_raw)
        product_name = row_data["product_name"]
        data_count = row_data["data_count"]
        spend = row_data["spend"]

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
                elif account_name_raw and cell_account.startswith(account_name_raw):
                    same_account = True

            if cell_date == target_date and same_account and cell_product == product_name:
                found_row_idx = i + 1  # 1-indexed
                found_cell_account = cell_account
                break

        if found_row_idx:
            worksheet.update(f"A{found_row_idx}", [[target_date]], value_input_option="USER_ENTERED")
            if found_cell_account != account_name:
                worksheet.update(f"B{found_row_idx}", [[account_name]])
                if len(all_values[found_row_idx - 1]) < 2:
                    all_values[found_row_idx - 1] = (all_values[found_row_idx - 1] + [""])[:2]
                all_values[found_row_idx - 1][1] = account_name
            worksheet.update(f"E{found_row_idx}:F{found_row_idx}", [[data_count, spend]])
        else:
            # Ghi tiep sau cum data logic de tranh lech xuong vung xa khong lien quan.
            next_row = logical_last_data_row + 1

            if next_row > worksheet.row_count:
                worksheet.add_rows(next_row - worksheet.row_count)

            # Chi ghi vao cac cot A, B, D, E, F; bo qua cot C
            worksheet.update(
                f"A{next_row}:B{next_row}",
                [[target_date, account_name]],
                value_input_option="USER_ENTERED",
            )
            worksheet.update(f"D{next_row}:F{next_row}", [[product_name, data_count, spend]])
            # Cap nhat cache local
            while len(all_values) < next_row:
                all_values.append([])
            all_values[next_row - 1] = [target_date, account_name, "", product_name, str(data_count), str(spend)]
            logical_last_data_row = next_row

        written += 1
        time.sleep(0.5)  # Tranh vuot rate limit Sheets API

    return written


def fill_all_sheets_command(sheet_urls_file: Optional[str], access_token: Optional[str], mode: str) -> int:
    token = (access_token or os.getenv("META_ACCESS_TOKEN", "")).strip()
    if not token:
        print("Thieu access token. Truyen --access-token hoac set META_ACCESS_TOKEN.")
        return 1

    if mode == "yesterday":
        date_preset = "yesterday"
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        date_preset = "today"
        target_date = datetime.now().strftime("%Y-%m-%d")

    date_vn = format_date_vn(target_date)

    file_path = Path(sheet_urls_file).resolve() if sheet_urls_file else SHEET_URLS_PATH
    sheet_urls = load_sheet_urls(file_path)
    if not sheet_urls:
        print("Khong co sheet nao trong file sheet_urls.")
        return 1

    total_written = 0

    for sheet_url in sheet_urls:
        print(f"\nDang xu ly sheet: {sheet_url}")
        sheet_result = inspect_sheet(sheet_url)
        print(f"  Owner: {sheet_result.owner_name} | So TK: {len(sheet_result.account_ids)}")

        rows_to_write = []

        for account_id in sheet_result.account_ids:
            try:
                campaigns = fetch_campaign_insights(account_id, token, date_preset)
                if not campaigns:
                    print(f"  [{account_id}] Khong co campaign co chi tieu.")
                    continue

                account_name = get_account_name_from_meta(account_id, token)
                print(f"  [{account_id}] {account_name}: {len(campaigns)} campaign")

                for camp in campaigns:
                    spend_val = float(camp.get("spend", 0) or 0)
                    if spend_val <= 0:
                        continue

                    campaign_name = camp.get("campaign_name", "")
                    product_name = extract_product_name(campaign_name)
                    data_count = sum_actions(camp.get("actions", []))

                    rows_to_write.append({
                        "date_vn": date_vn,
                        "account_id": account_id,
                        "account_name": account_name,
                        "product_name": product_name,
                        "data_count": data_count,
                        "spend": spend_val,
                    })
                    print(f"    -> {product_name}: data={data_count}, spend={spend_val:.0f}")

            except Exception as exc:
                print(f"  [{account_id}] Loi: {exc}")
                continue

        if rows_to_write:
            written = upsert_rows_to_chi_phi_ads(sheet_result.sheet_id, rows_to_write)
            total_written += written
            print(f"  Da ghi {written} dong vao sheet '{CHI_PHI_ADS_SHEET}'.")
        else:
            print("  Khong co dong nao de ghi.")

    print(f"\nHoan tat. Tong so dong da ghi: {total_written}")
    return 0


def fetch_today_spend_from_meta(account_id: str, access_token: str) -> float:
    account_id = normalize_account_id(account_id)
    endpoint = f"https://graph.facebook.com/{META_GRAPH_VERSION}/act_{account_id}/insights"
    params = {
        "fields": "spend",
        "date_preset": "today",
        "access_token": access_token,
        "limit": 1,
    }

    response = requests.get(endpoint, params=params, timeout=20)
    response.raise_for_status()
    payload = response.json()

    data = payload.get("data", [])
    if not data:
        return 0.0

    spend = data[0].get("spend", "0")
    return float(spend)


def fetch_today_spend_for_accounts(account_ids: List[str], access_token: str) -> List[AccountSpendResult]:
    results: List[AccountSpendResult] = []
    for account_id in account_ids:
        spend = fetch_today_spend_from_meta(account_id=account_id, access_token=access_token)
        results.append(AccountSpendResult(account_id=normalize_account_id(account_id), spend=spend))
    return results


def load_sheet_urls(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Khong tim thay file sheet url: {path}")

    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []

        # Uu tien cot sheet_url, fallback cot dau tien neu user dat ten cot khac.
        key = "sheet_url" if "sheet_url" in reader.fieldnames else reader.fieldnames[0]
        urls = []
        for row in reader:
            url = (row.get(key, "") or "").strip()
            if url:
                urls.append(url)

    # Loai trung, giu thu tu xuat hien.
    unique = []
    seen = set()
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique.append(url)
    return unique


def check_accounts_in_sheet(sheet_result: SheetInspectionResult, access_token: str) -> List[AccountCheckResult]:
    results: List[AccountCheckResult] = []
    for account_id in sheet_result.account_ids:
        normalized_id = normalize_account_id(account_id)
        try:
            spend = fetch_today_spend_from_meta(account_id=normalized_id, access_token=access_token)
            results.append(
                AccountCheckResult(
                    owner_name=sheet_result.owner_name,
                    sheet_id=sheet_result.sheet_id,
                    sheet_url=sheet_result.sheet_url,
                    account_id=normalized_id,
                    status="ok",
                    spend_today=spend,
                    error="",
                )
            )
        except Exception as exc:
            results.append(
                AccountCheckResult(
                    owner_name=sheet_result.owner_name,
                    sheet_id=sheet_result.sheet_id,
                    sheet_url=sheet_result.sheet_url,
                    account_id=normalized_id,
                    status="error",
                    spend_today=None,
                    error=str(exc),
                )
            )
    return results


def check_all_sheets_command(sheet_urls_file: Optional[str], access_token: Optional[str]) -> int:
    token = (access_token or os.getenv("META_ACCESS_TOKEN", "")).strip()
    if not token:
        print("Thieu access token. Truyen --access-token hoac set META_ACCESS_TOKEN.")
        return 1

    file_path = Path(sheet_urls_file).resolve() if sheet_urls_file else SHEET_URLS_PATH
    sheet_urls = load_sheet_urls(file_path)
    if not sheet_urls:
        print("Khong co sheet nao trong file sheet_urls.")
        return 1

    all_results: List[AccountCheckResult] = []
    for sheet_url in sheet_urls:
        sheet_result = inspect_sheet(sheet_url)
        all_results.extend(check_accounts_in_sheet(sheet_result, token))

    all_results.sort(key=lambda x: (x.owner_name, x.account_id, x.sheet_id))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_report_path = OUTPUT_DIR / f"full_sheet_account_check_{stamp}.csv"
    ready_path = OUTPUT_DIR / f"sheet_spend_ready_{stamp}.csv"

    full_rows = [
        {
            "owner_name": item.owner_name,
            "sheet_id": item.sheet_id,
            "sheet_url": item.sheet_url,
            "account_id": item.account_id,
            "status": item.status,
            "spend_today": "" if item.spend_today is None else f"{item.spend_today:.2f}",
            "error": item.error,
        }
        for item in all_results
    ]
    write_full_check_rows(full_report_path, full_rows)

    today = datetime.now().strftime("%Y-%m-%d")
    ready_rows = [
        {
            "date": today,
            "owner_name": item.owner_name,
            "sheet_id": item.sheet_id,
            "sheet_url": item.sheet_url,
            "account_id": item.account_id,
            "spend": f"{(item.spend_today or 0.0):.2f}",
        }
        for item in all_results
        if item.status == "ok"
    ]
    write_ready_rows(ready_path, ready_rows)

    ok_count = len([x for x in all_results if x.status == "ok"])
    error_count = len(all_results) - ok_count

    print("Da check toan bo ID tkqc trong tat ca sheet, khong bo sot.")
    print(f"sheet_count: {len(sheet_urls)}")
    print(f"total_checked: {len(all_results)}")
    print(f"ok_count: {ok_count}")
    print(f"error_count: {error_count}")
    print(f"full_report: {full_report_path}")
    print(f"ready_for_sheet: {ready_path}")
    return 0


def upsert_sheet_daily_spend_staging(
    sheet_result: SheetInspectionResult,
    spend_results: List[AccountSpendResult],
) -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    staging_path = OUTPUT_DIR / "sheet_daily_spend_staging.csv"
    rows = read_csv_rows(staging_path)

    key_set = {(today, sheet_result.sheet_id, result.account_id) for result in spend_results}
    kept_rows = []
    for row in rows:
        row_key = (
            row.get("date", ""),
            row.get("sheet_id", ""),
            normalize_account_id(row.get("account_id", "")),
        )
        if row_key not in key_set:
            kept_rows.append(row)

    for result in spend_results:
        kept_rows.append(
            {
                "date": today,
                "fetched_at": fetched_at,
                "sheet_id": sheet_result.sheet_id,
                "sheet_url": sheet_result.sheet_url,
                "owner_name": sheet_result.owner_name,
                "account_id": result.account_id,
                "spend": f"{result.spend:.2f}",
            }
        )

    kept_rows.sort(key=lambda row: (row["date"], row["owner_name"], row["account_id"]))
    write_spend_staging_rows(staging_path, kept_rows)
    return staging_path


def build_pending_sheet_update_file(
    sheet_result: SheetInspectionResult,
    spend_results: List[AccountSpendResult],
) -> Path:
    payload_dir = OUTPUT_DIR / "pending_sheet_updates"
    payload_dir.mkdir(parents=True, exist_ok=True)

    payload_date = datetime.now().strftime("%Y-%m-%d")
    payload = {
        "sheet_id": sheet_result.sheet_id,
        "sheet_url": sheet_result.sheet_url,
        "owner_name": sheet_result.owner_name,
        "date": payload_date,
        "worksheet_name": "Chi phí ADS",
        "note": "Cho map cot sau: day la payload tam de doi cau hinh cot chinh thuc.",
        "rows": [
            {
                "account_id": item.account_id,
                "spend": float(f"{item.spend:.2f}"),
            }
            for item in spend_results
        ],
    }

    output_path = payload_dir / f"{sheet_result.sheet_id}_{payload_date}.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return output_path


def inspect_sheet_command(sheet_url: str) -> int:
    result = inspect_sheet(sheet_url)
    print(f"owner_name: {result.owner_name}")
    print(f"sheet_id: {result.sheet_id}")
    print("account_ids:")
    for account_id in result.account_ids:
        print(account_id)
    return 0


def import_sheet_command(sheet_url: str) -> int:
    result = inspect_sheet(sheet_url)
    upsert_owner_mappings(result.owner_name, result.account_ids)

    print("Da cap nhat bang mapping tu Google Sheet.")
    print(f"owner_name: {result.owner_name}")
    print(f"sheet_id: {result.sheet_id}")
    print(f"account_count: {len(result.account_ids)}")
    return 0


def collect_sheet_spend_command(sheet_url: str) -> int:
    token = os.getenv("META_ACCESS_TOKEN", "").strip()
    if not token:
        print("Thieu META_ACCESS_TOKEN trong bien moi truong.")
        return 1

    sheet_result = inspect_sheet(sheet_url)
    spend_results = fetch_today_spend_for_accounts(sheet_result.account_ids, token)
    staging_path = upsert_sheet_daily_spend_staging(sheet_result, spend_results)
    pending_path = build_pending_sheet_update_file(sheet_result, spend_results)
    total_spend = sum(item.spend for item in spend_results)

    print("Da lay spend hom nay tu Meta API cho toan bo ID tkqc trong sheet.")
    print(f"owner_name: {sheet_result.owner_name}")
    print(f"sheet_id: {sheet_result.sheet_id}")
    print(f"account_count: {len(spend_results)}")
    print(f"total_spend: {total_spend:.2f}")
    print(f"staging_file: {staging_path}")
    print(f"pending_payload: {pending_path}")
    return 0


def lookup_command(account_id: str) -> int:
    owner_map = load_owner_map()
    owner = resolve_owner(account_id, owner_map)
    if not owner:
        print("Khong tim thay account_id trong bang mapping.")
        return 1

    print(f"account_id: {owner.account_id}")
    print(f"owner_name: {owner.owner_name}")
    return 0


def fill_now_command(account_id: str, spend: Optional[float], use_meta_api: bool) -> int:
    owner_map = load_owner_map()
    owner = resolve_owner(account_id, owner_map)
    if not owner:
        print("Khong tim thay account_id trong bang mapping.")
        return 1

    slots = load_schedule_slots()
    now = datetime.now()
    slot = get_current_slot(now, slots)
    date_str = now.strftime("%Y-%m-%d")

    final_spend = spend
    if final_spend is None:
        if not use_meta_api:
            print("Can --spend hoac bat --use-meta-api de lay chi phi tu Meta API.")
            return 1
        token = os.getenv("META_ACCESS_TOKEN", "").strip()
        if not token:
            print("Thieu META_ACCESS_TOKEN trong bien moi truong.")
            return 1
        final_spend = fetch_today_spend_from_meta(account_id=owner.account_id, access_token=token)

    upsert_daily_cost(
        date_str=date_str,
        time_slot=slot,
        account_id=owner.account_id,
        owner_name=owner.owner_name,
        spend=final_spend,
    )

    print("Da cap nhat chi phi ads.")
    print(f"date: {date_str}")
    print(f"time_slot: {slot}")
    print(f"account_id: {owner.account_id}")
    print(f"owner_name: {owner.owner_name}")
    print(f"spend: {final_spend:.2f}")
    return 0


def run_scheduler_command(account_id: str) -> int:
    owner_map = load_owner_map()
    owner = resolve_owner(account_id, owner_map)
    if not owner:
        print("Khong tim thay account_id trong bang mapping.")
        return 1

    slots = load_schedule_slots()
    token = os.getenv("META_ACCESS_TOKEN", "").strip()
    if not token:
        print("Thieu META_ACCESS_TOKEN trong bien moi truong.")
        return 1

    print("Bat dau scheduler. Nhan Ctrl+C de dung.")
    print(f"Tai khoan: {owner.account_id} - {owner.owner_name}")
    print(f"Khung gio: {', '.join(slots)}")

    executed_flags = set()
    try:
        while True:
            now = datetime.now()
            date_str = now.strftime("%Y-%m-%d")
            hm = now.strftime("%H:%M")

            for slot in slots:
                key = (date_str, slot)
                if hm == slot and key not in executed_flags:
                    spend = fetch_today_spend_from_meta(owner.account_id, token)
                    upsert_daily_cost(
                        date_str=date_str,
                        time_slot=slot,
                        account_id=owner.account_id,
                        owner_name=owner.owner_name,
                        spend=spend,
                    )
                    executed_flags.add(key)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Da ghi slot {slot}: {spend:.2f}")

            # Reset cache theo ngay de khong nho vo han.
            executed_flags = {item for item in executed_flags if item[0] == date_str}
            time.sleep(15)
    except KeyboardInterrupt:
        print("Da dung scheduler.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Tra owner theo account ID Facebook Ads va ghi chi phi theo khung gio"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    lookup_parser = subparsers.add_parser("lookup", help="Tra owner theo account ID")
    lookup_parser.add_argument("--account-id", required=True, help="ID tai khoan quang cao")

    inspect_parser = subparsers.add_parser(
        "inspect-sheet", help="Doc ten nhan su va ID tkqc tu link Google Sheet"
    )
    inspect_parser.add_argument("--sheet-url", required=True, help="Link Google Sheet")

    import_parser = subparsers.add_parser(
        "import-sheet", help="Doc Google Sheet va cap nhat bang mapping account_owner"
    )
    import_parser.add_argument("--sheet-url", required=True, help="Link Google Sheet")

    collect_spend_parser = subparsers.add_parser(
        "collect-sheet-spend",
        help="Lay spend hom nay cho tat ca ID tkqc trong Google Sheet va luu payload cho buoc ghi sheet",
    )
    collect_spend_parser.add_argument("--sheet-url", required=True, help="Link Google Sheet")

    check_all_parser = subparsers.add_parser(
        "check-all-sheets",
        help="Check full tat ca ID tkqc trong danh sach sheet va xuat file full/ready",
    )
    check_all_parser.add_argument(
        "--sheet-urls-file",
        help="Duong dan file CSV chua cot sheet_url. Mac dinh: storage/input/sheet_urls.csv",
    )
    check_all_parser.add_argument(
        "--access-token",
        help="Meta access token. Neu bo qua se doc tu bien moi truong META_ACCESS_TOKEN",
    )

    fill_all_parser = subparsers.add_parser(
        "fill-all-sheets",
        help="Lay du lieu Meta API va ghi vao tab 'Chi phi ADS' cho tat ca sheet",
    )
    fill_all_parser.add_argument(
        "--sheet-urls-file",
        help="Duong dan file CSV chua cot sheet_url. Mac dinh: storage/input/sheet_urls.csv",
    )
    fill_all_parser.add_argument(
        "--access-token",
        help="Meta access token. Neu bo qua se doc tu bien moi truong META_ACCESS_TOKEN",
    )
    fill_all_parser.add_argument(
        "--mode",
        choices=["today", "yesterday"],
        default="today",
        help="today: du lieu hom nay (realtime), yesterday: du lieu hom qua (chinh thuc 7h sang). Mac dinh: today",
    )

    fill_parser = subparsers.add_parser("fill-now", help="Ghi chi phi cho slot hien tai")
    fill_parser.add_argument("--account-id", required=True, help="ID tai khoan quang cao")
    fill_parser.add_argument("--spend", type=float, help="Chi phi ads (neu nhap tay)")
    fill_parser.add_argument(
        "--use-meta-api",
        action="store_true",
        help="Lay chi phi tu Meta API (can META_ACCESS_TOKEN)",
    )

    scheduler_parser = subparsers.add_parser(
        "run-scheduler", help="Tu dong ghi chi phi theo khung gio"
    )
    scheduler_parser.add_argument("--account-id", required=True, help="ID tai khoan quang cao")

    return parser


def main() -> int:
    ensure_runtime_directories()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "lookup":
        return lookup_command(args.account_id)
    if args.command == "inspect-sheet":
        return inspect_sheet_command(args.sheet_url)
    if args.command == "import-sheet":
        return import_sheet_command(args.sheet_url)
    if args.command == "collect-sheet-spend":
        return collect_sheet_spend_command(args.sheet_url)
    if args.command == "check-all-sheets":
        return check_all_sheets_command(args.sheet_urls_file, args.access_token)
    if args.command == "fill-all-sheets":
        return fill_all_sheets_command(args.sheet_urls_file, args.access_token, args.mode)
    if args.command == "fill-now":
        return fill_now_command(args.account_id, args.spend, args.use_meta_api)
    if args.command == "run-scheduler":
        return run_scheduler_command(args.account_id)

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
