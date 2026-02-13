import os, re, time, random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

# ✅ 国际版 Lark：open.larksuite.com
LARK_HOST = os.getenv("LARK_HOST", "https://open.larksuite.com").rstrip("/")

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("FEISHU_SHEET_TOKEN")  # spreadsheet token（不是整条URL）

# ✅ 如果你设置了 FEISHU_SHEET_NAME，就优先用它；否则自动用当月 "2月/3月..."
JST = timezone(timedelta(hours=9))
AUTO_MONTH_SHEET = f"{datetime.now(JST).month}月"
SHEET_NAME = os.getenv("FEISHU_SHEET_NAME") or AUTO_MONTH_SHEET

HEADER_ROW = 1
DATA_START_ROW = 2
ASIN_COL = "B"
MAX_ROWS = int(os.getenv("MAX_ROWS", "200"))

def sleep_jitter(a=1.5, b=3.5):
    time.sleep(random.uniform(a, b))

def today_header_text():
    now = datetime.now(JST)
    return f"{now.month}月{now.day}日"

def num_to_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(r + ord("A")) + s
    return s

def get_tenant_access_token() -> str:
    url = f"{LARK_HOST}/open-apis/auth/v3/tenant_access_token/internal/"
    r = requests.post(url, json={"app_id": APP_ID, "app_secret": APP_SECRET}, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][auth] {data}")
    return data["tenant_access_token"]

def batch_get(token: str, ranges: list[str]) -> dict:
    url = f"{LARK_HOST}/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_batch_get"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, params={"ranges": ranges}, timeout=30)

    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            j = r.text
        raise RuntimeError(f"[LARK][batch_get] status={r.status_code} body={j}")

    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][batch_get] {data}")
    return data

def batch_update(token: str, updates: list[dict]) -> None:
    url = f"{LARK_HOST}/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_batch_update"
    headers = {"Authorization": f"Bearer {token}"}
    body = {"valueInputOption": "RAW", "data": updates}
    r = requests.post(url, headers=headers, json=body, timeout=30)

    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            j = r.text
        raise RuntimeError(f"[LARK][batch_update] status={r.status_code} body={j}")

    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][batch_update] {data}")

def list_sheets(token: str) -> list[dict]:
    # ✅ 查 spreadsheet 内所有 sheet tab
    url = f"{LARK_HOST}/open-apis/sheets/v3/spreadsheets/{SPREADSHEET_TOKEN}/sheets/query"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=30)

    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            j = r.text
        raise RuntimeError(f"[LARK][list_sheets] status={r.status_code} body={j}")

    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][list_sheets] {data}")

    return data.get("data", {}).get("sheets", []) or []

def ensure_sheet_tab_exists(token: str, sheet_name: str) -> None:
    sheets = list_sheets(token)
    for s in sheets:
        if s.get("title") == sheet_name:
            return  # ✅ 已存在

    # ✅ 不存在就创建
    url = f"{LARK_HOST}/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/sheets_batch_update"
    headers = {"Authorization": f"Bearer {token}"}
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    r = requests.post(url, headers=headers, json=body, timeout=30)
    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            j = r.text
        raise RuntimeError(f"[LARK][add_sheet] status={r.status_code} body={j}")

    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][add_sheet] {data}")

    print(f"[INFO] created sheet tab: {sheet_name}")

def extract_asin(v) -> str | None:
    s = str(v).strip() if v is not None else ""
    m = re.search(r"/dp/([A-Z0-9]{10})", s)
    if m:
        return m.group(1)
    m = re.search(r"\b([A-Z0-9]{10})\b", s)
    if m:
        return m.group(1)
    return None

def ensure_today_col(token: str) -> str:
    rng = f"{SHEET_NAME}!A{HEADER_ROW}:ZZ{HEADER_ROW}"
    data = batch_get(token, [rng])
    row = (data["data"]["valueRanges"][0].get("values") or [[]])[0]
    today = today_header_text()

    last = 0
    for i, v in enumerate(row):
        sv = str(v).strip() if v is not None else ""
        if sv:
            last = i + 1
        if sv == today:
            return num_to_col(i + 1)

    target = last + 1
    col = num_to_col(target)
    batch_update(token, [{
        "range": f"{SHEET_NAME}!{col}{HEADER_ROW}:{col}{HEADER_ROW}",
        "values": [[today]]
    }])
    return col

def fetch_rank(asin: str) -> tuple[str | None, str]:
    url = f"https://www.amazon.co.jp/dp/{asin}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept-Language": "ja-JP,ja;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=25)
    if r.status_code == 403:
        return None, "HTTP_403"
    if r.status_code != 200:
        return None, f"HTTP_{r.status_code}"

    html = r.text
    if "captcha" in html.lower() or "ロボットではありません" in html:
        return None, "CAPTCHA?"

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text("\n", strip=True)

    m = re.search(r"([^\n]{2,80})\s*-\s*(\d{1,3}(?:,\d{3})*)位", text)
    if not m:
        return None, "RANK_N/A"
    return f"{m.group(1)} - {m.group(2)}位", "OK"

def main():
    if not (APP_ID and APP_SECRET and SPREADSHEET_TOKEN):
        raise RuntimeError("Missing env vars: FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_SHEET_TOKEN")

    print(f"[INFO] host={LARK_HOST}")
    print(f"[INFO] sheet_name={SHEET_NAME}")

    token = get_tenant_access_token()

    # ✅ 关键：先确保当月 tab 存在（否则 NOTEXIST）
    ensure_sheet_tab_exists(token, SHEET_NAME)

    col = ensure_today_col(token)
    print(f"[INFO] today={today_header_text()} col={col}")

    end_row = DATA_START_ROW + MAX_ROWS - 1
    asin_rng = f"{SHEET_NAME}!{ASIN_COL}{DATA_START_ROW}:{ASIN_COL}{end_row}"
    data = batch_get(token, [asin_rng])
    rows = data["data"]["valueRanges"][0].get("values") or []

    updates = []
    for i, r in enumerate(rows):
        row_no = DATA_START_ROW + i
        asin = extract_asin(r[0] if r else "")
        if not asin:
            continue

        val, status = None, "RANK_N/A"
        for _ in range(2):
            val, status = fetch_rank(asin)
            if val:
                break
            sleep_jitter()

        updates.append({
            "range": f"{SHEET_NAME}!{col}{row_no}:{col}{row_no}",
            "values": [[val or status]]
        })
        print({"row": row_no, "asin": asin, "write": val or status})
        sleep_jitter()

    if updates:
        batch_update(token, updates)
        print(f"[DONE] updated {len(updates)} rows")
    else:
        print("[DONE] nothing to update (no ASIN found)")

if __name__ == "__main__":
    main()

