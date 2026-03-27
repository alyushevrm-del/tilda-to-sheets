import os
import json
import logging

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

# Bus options: (capacity, cost_rub)
BUSES = [
    (18, 16750),
    (20, 20500),
    (25, 20250),
    (35, 29250),
    (49, 37500),
    (58, 45000),
]

def calculate_transfer_cost(total_people: int) -> int:
    """Return minimum total bus cost to seat all people using DP."""
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

def get_worksheet(turnir: str) -> gspread.Worksheet:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        info = json.loads(creds_json, strict=False)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(
            "/etc/secrets/credentials.json", scopes=SCOPES
        )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    return spreadsheet.worksheet(turnir)

def find_first_empty_row(ws: gspread.Worksheet) -> int:
    col_a = ws.col_values(1)
    for row_idx in range(FIRST_DATA_ROW, len(col_a) + 1):
        if row_idx > len(col_a) or col_a[row_idx - 1] == "":
            return row_idx
    return len(col_a) + 1

@app.post("/webhook")
async def webhook(request: Request):
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await request.json()
        else:
            form = await request.form()
            data = dict(form)
        logger.info("Received data: %s", data)
        turnir = (data.get("turnir") or "").strip()
        if not turnir:
            logger.info("Empty turnir - ignoring test request")
            return JSONResponse({"status": "ok", "ignored": True})
        name           = (data.get("name")            or "").strip()
        phone          = (data.get("phone")           or "").strip()
        email          = (data.get("email")           or "").strip()
        name_team      = (data.get("name_team")       or "").strip()
        format_oplaty  = (data.get("format_oplaty")   or "").strip()
        date_start     = (data.get("date_start")      or "").strip()
        time_start     = (data.get("time_start")      or "").strip()
        info_pribytie  = (data.get("info_pribytie")   or "").strip()
        info_otpravlenye = (data.get("info_otpravlenye") or "").strip()
        date_end       = (data.get("date_end")        or "").strip()
        time_end       = (data.get("time_end")        or "").strip()
        kol_detey      = (data.get("kol_detey")       or "").strip()
        kol_trener     = (data.get("kol_trener")      or "").strip()
        kol_parent     = (data.get("kol_parent")      or "").strip()
        transfer       = (data.get("transfer")        or "").strip()
        team_contact  = f"{name_team}, {name}" if name_team and name else (name_team or name)
        arrival_dt    = f"{date_start} {time_start}".strip()
        departure_dt  = f"{date_end} {time_end}".strip()
        departure_info = " ".join(filter(None, [date_end, time_end, info_otpravlenye]))
        # Transfer cost calculation
        if transfer.lower().startswith("да"):
            try:
                total_people = (
                    int(kol_detey or 0)
                    + int(kol_trener or 0)
                    + int(kol_parent or 0)
                )
            except ValueError:
                total_people = 0
            transfer_cost = calculate_transfer_cost(total_people)
        else:
            transfer_cost = 0
        ws = get_worksheet(turnir)
        row = find_first_empty_row(ws)
        serial = row - FIRST_DATA_ROW + 1
        logger.info("Writing to sheet '%s', row %d", turnir, row)
        updates = [
            {"range": f"A{row}",  "values": [[serial]]},
            {"range": f"B{row}",  "values": [[team_contact]]},
            {"range": f"C{row}",  "values": [[arrival_dt]]},
            {"range": f"D{row}",  "values": [[departure_dt]]},
            {"range": f"G{row}",  "values": [[kol_detey]]},
            {"range": f"H{row}",  "values": [[kol_trener]]},
            {"range": f"I{row}",  "values": [[kol_parent]]},
            {"range": f"BN{row}", "values": [[transfer_cost]]},
            {"range": f"BQ{row}", "values": [[format_oplaty]]},
            {"range": f"BT{row}", "values": [[departure_info]]},
            {"range": f"BU{row}", "values": [[departure_dt]]},
            {"range": f"BV{row}", "values": [[name]]},
            {"range": f"BW{row}", "values": [[phone]]},
            {"range": f"BX{row}", "values": [[email]]},
        ]
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        logger.info("Successfully written row %d to sheet '%s'", row, turnir)
        return JSONResponse({"status": "ok", "sheet": turnir, "row": row})
    except gspread.exceptions.WorksheetNotFound:
        logger.error("Worksheet '%s' not found", turnir)
        return JSONResponse({"status": "error", "detail": f"Worksheet '{turnir}' not found"})
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return JSONResponse({"status": "error", "detail": str(exc)})

@app.get("/")
def healthcheck():
    return {"status": "running"}
