from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")


def col_letter_to_index(col: str) -> int:
    col = col.upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result


def get_sheet(sheet_name: str):
    creds = Credentials.from_service_account_file(
        "/etc/secrets/credentials.json", scopes=SCOPES
    )
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        raise ValueError(f"Лист '{sheet_name}' не найден в таблице.")
    return sheet


def find_next_empty_row(sheet) -> int:
    col_a = sheet.col_values(1)
    for i in range(4, len(col_a)):
        if not str(col_a[i]).strip():
            return i + 1
    return max(len(col_a) + 1, 5)


def parse_tilda_data(data: dict) -> dict:
    return {k.lower().strip(): str(v).strip() if v else "" for k, v in data.items()}


def build_row_data(parsed: dict) -> dict:
    команда = parsed.get("name_team", "")
    имя = parsed.get("name", "")
    if имя:
        команда = f"{команда}, {имя}"

    заезд = f"{parsed.get('date_start', '')} {parsed.get('time_start', '')}".strip()
    выезд = f"{parsed.get('date_end', '')} {parsed.get('time_end', '')}".strip()
    отправление = f"{parsed.get('date_end', '')} {parsed.get('time_end', '')}".strip()

    return {
        "B":  команда,
        "C":  заезд,
        "D":  выезд,
        "G":  parsed.get("kol_detey", ""),
        "H":  parsed.get("kol_trener", ""),
        "I":  parsed.get("kol_parent", ""),
        "BN": parsed.get("transfer", ""),
        "BQ": parsed.get("format_oplaty", ""),
        "BT": parsed.get("info_pribytie", ""),
        "BU": отправление,
        "BV": parsed.get("name", ""),
        "BW": parsed.get("phone", ""),
        "BX": parsed.get("email", ""),
    }


@app.get("/")
async def root():
    return {"status": "ok", "message": "Tilda -> Google Sheets webhook работает"}


@app.post("/webhook")
async def webhook(request: Request):
    # Всегда отвечаем 200 чтобы Тильда приняла вебхук
    try:
        content_type = request.headers.get("content-type", "")
        if "application/json" in content_type:
            data = await request.json()
        else:
            form = await request.form()
            data = dict(form)

        logger.info(f"Получена заявка: {data}")
        parsed = parse_tilda_data(data)

        sheet_name = parsed.get("turnir", "").strip()

        # Если поле turnir пустое — это тестовый запрос от Тильды, игнорируем
        if not sheet_name:
            logger.info("Поле turnir пустое — тестовый запрос, пропускаем")
            return JSONResponse(content={"status": "ok", "info": "test request"}, status_code=200)

        sheet = get_sheet(sheet_name)
        next_row = find_next_empty_row(sheet)
        logger.info(f"Записываем в лист '{sheet_name}', строка {next_row}")

        sheet.update_cell(next_row, col_letter_to_index("A"), next_row - 4)

        row_data = build_row_data(parsed)
        for col_letter, value in row_data.items():
            if value:
                col_idx = col_letter_to_index(col_letter)
                sheet.update_cell(next_row, col_idx, value)

        logger.info(f"Готово: лист '{sheet_name}', строка {next_row}")
        return JSONResponse(
            content={"status": "ok", "sheet": sheet_name, "row": next_row},
            status_code=200
        )

    except Exception as e:
        # Даже при ошибке отвечаем 200 чтобы Тильда не блокировала вебхук
        logger.error(f"Ошибка: {e}", exc_info=True)
        return JSONResponse(
            content={"status": "error", "detail": str(e)},
            status_code=200
        )
