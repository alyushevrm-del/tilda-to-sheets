import os
import json
import logging
import re
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
FIRST_DATA_ROW = 5
app = FastAPI(title="Tilda to Google Sheets webhook")

BUSES = [
    (18, 16750),
    (20, 20500),
    (25, 20250),
    (35, 29250),
    (49, 37500),
    (58, 45000),
]

MEAL_MAP = {
    "завтрак": 0,
    "обед": 1,
    "полдник": 2,
    "ужин": 3,
    "второй ужин": 4,
}
STANDARD_MEAL_OFFSETS = [0, 1, 3]  # з, о, у

def calculate_transfer_cost(total_people: int) -> int:
    if total_people <= 0:
        return 0
    INF = float("inf")
    dp = [INF] * (total_people + 1)
    dp[0] = 0
    for n in range(1, total_people + 1):
        for cap, cost in BUSES:
            if cap >= n:
                dp[n] = min(dp[n], cost)
            elif n - cap >= 0 and dp[n - cap] < INF:
                dp[n] = min(dp[n], dp[n - cap] + cost)
    return dp[total_people] if dp[total_people] < INF else 0

def normalize_phone(phone: str) -> str:
    if phone.startswith("+7"):
        return "8" + phone[2:]
    return phone

def col_num_to_letter(n: int) -> str:
    result = ""
    while n > 0:
        n -= 1
        result = chr(ord("A") + n % 26) + result
        n //= 26
    return result

def parse_meal_offsets(meals_str: str) -> set:
    offsets = set()
    for m in re.split(r"[,;\n]", meals_str or ""):
        m = m.strip().lower()
        if m in MEAL_MAP:
            offsets.add(MEAL_MAP[m])
    return offsets

def build_meal_updates(ws, date_start_str, date_end_str, pitanie_start, pitanie_end, total_people, row):
    if not date_start_str or not date_end_str or total_people <= 0:
        return []
    try:
        d_start = datetime.strptime(date_start_str.strip(), "%d.%m.%Y")
        d_end = datetime.strptime(date_end_str.strip(), "%d.%m.%Y")
    except ValueError:
        return []
    header3 = ws.row_values(3)
    date_pattern = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    date_col_map = {}
    for i, val in enumerate(header3):
        val = str(val).strip()
        if date_pattern.match(val):
            date_col_map[val] = i + 1  # 1-indexed
    if not date_col_map:
        return []
    meals_arrival = parse_meal_offsets(pitanie_start)
    meals_departure = parse_meal_offsets(pitanie_end)
    standard = set(STANDARD_MEAL_OFFSETS)
    updates = []
    current = d_start
    while current <= d_end:
        date_key = current.strftime("%d.%m.%Y")
        if date_key in date_col_map:
            base_col = date_col_map[date_key]
            if current == d_start and current == d_end:
                active = meals_arrival | meals_departure if (meals_arrival or meals_departure) else standard
            elif current == d_start:
                active = meals_arrival if meals_arrival else standard
            elif current == d_end:
                active = meals_departure if meals_departure else standard
            else:
                active = standard
            for offset in sorted(active):
                col_letter = col_num_to_letter(base_col + offset)
                updates.append({"range": f"{col_letter}{row}", "values": [[total_people]]})
        current += timedelta(days=1)
    return updates

def get_worksheet(turnir: str) -> gspread.Worksheet:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json, strict=False)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file("/etc/secrets/credentials.json", scopes=SCOPES)
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(turnir)

def find_first_empty_row(ws: gspread.Worksheet) -> int:
    col_b = ws.col_values(2)
    for row_idx in range(FIRST_DATA_ROW, len(col_b) + 2):
        val = col_b[row_idx - 1] if row_idx - 1 < len(col_b) else ""
        if val == "":
            return row_idx
    return max(len(col_b) + 1, FIRST_DATA_ROW)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await request.json()
        else:
            form = await request.form()
            data = dict(form)
        turnir = (data.get("turnir") or "").strip()
        if not turnir:
            return JSONResponse({"status": "ok", "ignored": True})
        name             = (data.get("name")             or "").strip()
        phone            = normalize_phone((data.get("phone") or "").strip())
        email            = (data.get("email")            or "").strip()
        name_team        = (data.get("name_team")        or "").strip()
        format_oplaty    = (data.get("format_oplaty")    or "").strip()
        date_start       = (data.get("date_start")       or "").strip()
        time_start       = (data.get("time_start")       or "").strip()
        info_pribytie    = (data.get("info_pribytie")    or "").strip()
        info_otpravlenye = (data.get("info_otpravlenye") or "").strip()
        date_end         = (data.get("date_end")         or "").strip()
        time_end         = (data.get("time_end")         or "").strip()
        kol_detey        = (data.get("kol_detey")        or "").strip()
        kol_trener       = (data.get("kol_trener")       or "").strip()
        kol_parent       = (data.get("kol_parent")       or "").strip()
        transfer         = (data.get("transfer")         or "").strip()
        pitanie_start    = (data.get("pitanie_start")    or "").strip()
        pitanie_end      = (data.get("pitanie_end")      or "").strip()
        pitanie_syh_end  = (data.get("pitanie_syh_end")  or "").strip()
        team_contact   = f"{name_team}, {name}" if name_team and name else (name_team or name)
        arrival_dt     = f"{date_start} {time_start}".strip()
        departure_dt   = f"{date_end} {time_end}".strip()
        try:
            total_people = int(kol_detey or 0) + int(kol_trener or 0) + int(kol_parent or 0)
        except ValueError:
            total_people = 0
        transfer_cost = calculate_transfer_cost(total_people) if transfer.lower().startswith("да") else 0
        suh_paek_val = "да" if pitanie_syh_end.lower().startswith("да") else ("нет" if pitanie_syh_end else "")
        ws  = get_worksheet(turnir)
        row = find_first_empty_row(ws)
        serial = row - FIRST_DATA_ROW + 1
        updates = [
            {"range": f"A{row}",  "values": [[serial]]},
            {"range": f"B{row}",  "values": [[team_contact]]},
            {"range": f"C{row}",  "values": [[arrival_dt]]},
            {"range": f"D{row}",  "values": [[departure_dt]]},
            {"range": f"G{row}",  "values": [[kol_detey]]},
            {"range": f"H{row}",  "values": [[kol_trener]]},
            {"range": f"I{row}",  "values": [[kol_parent]]},
            {"range": f"BL{row}", "values": [[suh_paek_val]]},
            {"range": f"BN{row}", "values": [[transfer_cost]]},
            {"range": f"BQ{row}", "values": [[format_oplaty]]},
            {"range": f"BT{row}", "values": [[info_pribytie]]},
            {"range": f"BU{row}", "values": [[info_otpravlenye]]},
            {"range": f"BV{row}", "values": [[name]]},
            {"range": f"BW{row}", "values": [[phone]]},
            {"range": f"BX{row}", "values": [[email]]},
        ]
        meal_updates = build_meal_updates(ws, date_start, date_end, pitanie_start, pitanie_end, total_people, row)
        updates.extend(meal_updates)
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        return JSONResponse({"status": "ok", "sheet": turnir, "row": row})
    except gspread.exceptions.WorksheetNotFound:
        return JSONResponse({"status": "error", "detail": f"Worksheet '{turnir}' not found"})
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return JSONResponse({"status": "error", "detail": str(exc)})

@app.get("/")
def healthcheck():
    return {"status": "running"}
