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

## 5) Lich gui bao cao Telegram bang GitHub Actions

Project da chuyen sang dung GitHub Actions de goi lich gui bao cao Telegram thay vi Render Cron.

Workflow nam o `.github/workflows/telegram-reports.yml` va chay vao cac moc lech `07` va `37` phut de tranh gio cao diem cua scheduler GitHub. Lich duoc dat theo mui gio `Asia/Ho_Chi_Minh` va bao phu tu 6h den 23h moi ngay.

Can cau hinh them 1 repo secret tren GitHub:

- `INTERNAL_CRON_SECRET`: phai trung voi bien moi truong `INTERNAL_CRON_SECRET` dang dat tren Render web service `chi-phi-ads-dashboard`.

Sau khi dat secret, co the test tay trong tab Actions bang `Run workflow` va bat `force=true` neu can bo qua kiem tra slot da gui.

## 6) Tu dong ket noi Meta theo bang mapping (de nghi dung)

Muc tieu: web tu doc danh sach tkqc, tu chon token dung theo `token_key`, tu dong bo chi phi. Nhan vien chi can xu ly khi bi loi quyen/token.

### Buoc 1: Tao tab mapping trong Google Sheet

Tao 1 tab ten `Meta_Account_Map` (hoac `Meta Account Map`) voi cac cot:

- `owner_username`
- `meta_account_id` (co the nhap `745...` hoac `act_745...`)
- `account_label`
- `token_key`
- `is_active` (`TRUE/FALSE`)
- `priority` (so nho hon thi uu tien cao hon)

Vi du:

```csv
owner_username,meta_account_id,account_label,token_key,is_active,priority
emp_thang,745204308086518,HKD Nguyen Thi Thanh Hien,team_3,TRUE,1
emp_thang,1529748470808306,HKD Bui Thi Thu Loan,team_3,TRUE,2
```

### Buoc 2: Cau hinh token vault tren server

Dat bien moi truong `META_TOKEN_VAULT_PATH` tro toi file JSON (mac dinh: `storage/config/meta_token_vault.json`).

Noi dung mau:

```json
{
   "default": "EAAB...",
   "tokens": {
      "team_3": "EAAB...",
      "team_1": "EAAC..."
   }
}
```

Thu tu lay token:
1. token theo `token_key`
2. `default` trong vault
3. `META_ACCESS_TOKEN` (fallback cu)

### Buoc 3: Van hanh

- Frontend goi `/api/fetch-data` voi `sync_meta=true`.
- Backend tu dong:
   - doc `Meta_Account_Map`
   - loc theo `owner_username`
   - dong bo tung `meta_account_id` bang token phu hop
   - bo qua tai khoan loi va van tra du lieu cac tai khoan con lai

### Buoc 4: Khi nao nhan vien can thao tac

Chi can thao tac khi:

- loi quyen (`ads_read`) trong BM/ad account
- token het han/bi revoke
- thieu dong mapping trong `Meta_Account_Map`

Con lai he thong tu chay.
