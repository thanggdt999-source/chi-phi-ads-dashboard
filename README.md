# Facebook Ads Cost Filler

Phan mem nho de:
- Tra nguoi phu trach theo account ID Facebook Ads.
- Tu doc ten nhan su va ID tkqc tu link Google Sheet.
- Ghi chi phi ads theo khung gio co dinh.
- Co the lay chi phi tu Meta API neu da cau hinh token.

## 1) Cai dat

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## 2) Cau hinh

1. Sua file `storage/input/account_owner.csv` theo format:
   - `account_id,owner_name`
2. Sua file `config/schedule.json` de dat khung gio mong muon.
3. Tao bien moi truong `META_ACCESS_TOKEN` (tham khao `.env.example`).

## Cau truc luu tru tren o E

Toan bo du lieu cua app duoc luu ben trong thu muc project tren o E:

- `storage/input`: file dau vao, mapping account -> owner
- `storage/output`: file ket qua xuat ra
- `storage/logs`: log chay scheduler va log dong bo sau nay
- `storage/credentials`: credential cho Google Sheets API sau nay

## 3) Cac lenh su dung

### Tra owner theo account ID

```bash
python app.py lookup --account-id 123456789012345
```

### Doc ten nhan su va ID tkqc tu Google Sheet

```bash
python app.py inspect-sheet --sheet-url "https://docs.google.com/spreadsheets/d/.../edit"
```

Rule hien tai:
- Ten NV ADS la phan sau dau gach ngang cuoi cung trong tieu de file sheet.
- ID tkqc duoc lay trong sheet `Cài đặt`, cot G, o phan nam trong dau ngoac `(...)`.

### Nhap mapping tu Google Sheet vao app

```bash
python app.py import-sheet --sheet-url "https://docs.google.com/spreadsheets/d/.../edit"
```

Lenh nay se tu cap nhat file `storage/input/account_owner.csv`.

### Lay spend hom nay cho toan bo ID trong 1 sheet

```bash
set META_ACCESS_TOKEN=your_token_here
python app.py collect-sheet-spend --sheet-url "https://docs.google.com/spreadsheets/d/.../edit"
```

Lenh nay se tao 2 file:
- `storage/output/sheet_daily_spend_staging.csv`: du lieu spend tong hop theo ngay/account.
- `storage/output/pending_sheet_updates/<sheet_id>_<date>.json`: payload tam de doi map cot ghi len Google Sheet.

Khi dai ca chot cot can ghi, em se dung payload nay de day vao dung vi tri tren sheet.

### Check full tat ca sheet (khong bo sot ID)

Tao file `storage/input/sheet_urls.csv` theo mau:

```csv
sheet_url
https://docs.google.com/spreadsheets/d/.../edit
https://docs.google.com/spreadsheets/d/.../edit
```

Chay 1 lenh:

```bash
python app.py check-all-sheets --access-token YOUR_TOKEN
```

Ket qua xuat ra:
- `storage/output/full_sheet_account_check_<timestamp>.csv`: bao cao full tat ca ID (ok + error)
- `storage/output/sheet_spend_ready_<timestamp>.csv`: chi cac ID ok, san sang ghi len Google Sheet

### Ghi chi phi cho slot hien tai

Nhap tay chi phi:

```bash
python app.py fill-now --account-id 123456789012345 --spend 1250000
```

Lay tu Meta API:

```bash
set META_ACCESS_TOKEN=your_token_here
python app.py fill-now --account-id 123456789012345 --use-meta-api
```

### Chay tu dong theo khung gio

```bash
set META_ACCESS_TOKEN=your_token_here
python app.py run-scheduler --account-id 123456789012345
```

Khi den dung gio trong `config/schedule.json`, he thong se cap nhat vao `storage/output/daily_ads_cost.csv`.

## 4) Dau ra

File ket qua: `storage/output/daily_ads_cost.csv`

Cac cot:
- `date`
- `time_slot`
- `account_id`
- `owner_name`
- `spend`
