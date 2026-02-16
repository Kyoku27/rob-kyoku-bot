import os
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

# ✅ 国际版 Lark：open.larksuite.com
LARK_HOST = os.getenv("LARK_HOST", "https://open.larksuite.com").rstrip("/")

APP_ID = os.getenv("FEISHU_APP_ID")
APP_SECRET = os.getenv("FEISHU_APP_SECRET")
SPREADSHEET_TOKEN = os.getenv("FEISHU_SHEET_TOKEN")  # /sheets/{这里} 这一段

# ✅ 你现在 tab 名叫 “2月”，后面自动会用 “3月 / 4月 …”
JST = timezone(timedelta(hours=9))
AUTO_MONTH_SHEET = f"{datetime.now(JST).month}月"
SHEET_TITLE = os.getenv("FEISHU_SHEET_NAME") or AUTO_MONTH_SHEET

HEADER_ROW = 1
DATA_START_ROW = 2
ASIN_COL = "B"
MAX_ROWS = int(os.getenv("MAX_ROWS", "200"))


def sleep_jitter(a: float = 1.2, b: float = 2.8) -> None:
    time.sleep(random.uniform(a, b))


def today_header_text() -> str:
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


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }


def list_sheets(token: str) -> list[dict]:
    """
    ✅ 关键：拿到每个 tab 的 sheet_id + title
    """
    url = f"{LARK_HOST}/open-apis/sheets/v3/spreadsheets/{SPREADSHEET_TOKEN}/sheets/query"
    r = requests.get(url, headers=_headers(token), timeout=20)
    if r.status_code != 200:
        try:
            j = r.json()
        except Exception:
            j = r.text
        raise RuntimeError(f"[LARK][list_sheets] status={r.status_code} body={j}")

    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"[LARK][list_sheets] {data}")

    # data["data"]["sheets"] = [{ "sheet_id": "...", "title": "2月", ... }, ...]
    return data["data"].get("sheets", [])


def resolve_sheet_id(token: str, title: str) -> str:
    sheets = list_sheets(token)
    for s in sheets:
        if s.get("title") == title:
            return s.get("sheet_id")

    titles = [x.get("title") for x in sheets]
    raise RuntimeError(f"[LARK] sheet title not found: '{title}'. existing titles={titles}")


def batch_get(token: str, ranges: list[str]) -> dict:
    url = f"{LARK_HOST}/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_batch_get"
    r = requests.get(url, headers=_headers(token), params={"ranges": ranges}, timeout=30)
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


def batch_update(token: str, updates: list[dict]) -> dict:
    """
    updates: [{"range": "sheetId!A1:A1", "values": [[...]]}, ...]
    """
    url = f"{LARK_HOST}/open-apis/sheets/v2/spreadsheets/{SPREADSHEET_TOKEN}/values_batch_update"
    headers = _headers(token)

    body = {
        "valueInputOption": "RAW",
        "valueRanges": updates,  # ✅ 注意是 valueRanges
    }

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

    return data


def extract_asin(v) -> str | None:
    s = str(v).strip() if v is not None else ""
    m = re.search(r"/dp/([A-Z0-9]{10})", s)
    if m:
        return m.group(1)
    m = re.search(r"\b([A-Z0-9]{10})\b", s)
    if m:
        return m.group(1)
    return None


def ensure_today_col(token: str, sheet_id: str) -> str:
    # ✅ range 必须用 sheet_id，不是 “2月”
    rng = f"{sheet_id}!A{HEADER_ROW}:ZZ{HEADER_ROW}"
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

    batch_update(
        token,
        [
            {
                "range": f"{sheet_id}!{col}{HEADER_ROW}:{col}{HEADER_ROW}",
                "values": [[today]],
            }
        ],
    )
    return col


def fetch_rank(asin: str) -> tuple[str | None, str]:
    url = f"https://www.amazon.co.jp/dp/{asin}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
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

    # ⚠️ 这个正则是“尽量猜测”一段像：{类目名} - {排名}位
    m = re.search(r"([^\n]{2,80})\s*-\s*(\d{1,3}(?:,\d{3})*)位", text)
    if not m:
        return None, "RANK_N/A"
    return f"{m.group(1)} - {m.group(2)}位", "OK"


def main() -> None:
    if not (APP_ID and APP_SECRET and SPREADSHEET_TOKEN):
        raise RuntimeError("Missing env vars: FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_SHEET_TOKEN")

    print(f"[INFO] host={LARK_HOST}")
    print(f"[INFO] spreadsheet_token={SPREADSHEET_TOKEN}")
    print(f"[INFO] sheet_title={SHEET_TITLE}")

    token = get_tenant_access_token()

    # ✅ 关键：把 “2月” 转成 sheet_id
    sheet_id = resolve_sheet_id(token, SHEET_TITLE)
    print(f"[INFO] sheet_id={sheet_id}")

    col = ensure_today_col(token, sheet_id)
    print(f"[INFO] today={today_header_text()} col={col}")

    end_row = DATA_START_ROW + MAX_ROWS - 1
    asin_rng = f"{sheet_id}!{ASIN_COL}{DATA_START_ROW}:{ASIN_COL}{end_row}"
    data = batch_get(token, [asin_rng])
    rows = data["data"]["valueRanges"][0].get("values") or []

    updates: list[dict] = []
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

        updates.append(
            {
                "range": f"{sheet_id}!{col}{row_no}:{col}{row_no}",
                "values": [[val or status]],
            }
        )
        print({"row": row_no, "asin": asin, "write": val or status})
        sleep_jitter()

    if updates:
        batch_update(token, updates)
        print(f"[DONE] updated {len(updates)} rows")
    else:
        print("[DONE] nothing to update (no ASIN found)")


if __name__ == "__main__":
    main()
