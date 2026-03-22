from fastapi import FastAPI, Request, HTTPException
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
    """Переводим буквы колонки в числовой индекс (A=1, B=2, BN=66...)"""
    col = col.upper()
    result = 0
    for char in col:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result


def get_sheet(sheet_name: str):
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        raise ValueError(f"Лист '{sheet_name}' не найден в таблице. Проверьте название турнира в заявке.")

    return sheet


def find_next_empty_row(sheet) -> int:
    """Находим первую пустую строку в колонке A начиная с 5й строки (данные с row=5)"""
    col_a = sheet.col_values(1)  # Колонка A
    for i in range(4, len(col_a)):  # Начинаем с индекса 4 = строка 5
        if not str(col_a[i]).strip():
            return i + 1  # gspread индексирует с 1
    return max(len(col_a) + 1, 5)


def parse_tilda_data(data: dict) -> dict:
    """Нормализуем ключи из Тильды"""
    return {k.lower().strip(): str(v).strip() if v else "" for k, v in data.items()}


def build_row_data(parsed: dict) -> dict:
    """Формируем словарь {буква_колонки: значение}"""

    # B — Команда: название + ФИО руководителя
    команда = parsed.get("name_team", "")
    имя = parsed.get("name", "")
    if имя:
        команда = f"{команда}, {имя}"

    # C — Заезд: "22.03.2026 12:05"
    заезд = f"{parsed.get('date_start', '')} {parsed.get('time_start', '')}".strip()

    # D — Выезд: "25.03.2026 11:30"
    выезд = f"{parsed.get('date_end', '')} {parsed.get('time_end', '')}".strip()

    # BU — рейс/поезд при отправлении (дата + время выезда)
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
    return {"status": "ok", "message": "Tilda → Google Sheets webhook работает"}


@app.post("/webhook")
async def webhook(request: Request):
    try:
        content_type = request.headers.get("content-type", "")

        if "application/json" in content_type:
            data = await request.json()
        else:
            form = await request.form()
            data = dict(form)

        logger.info(f"Получена заявка: {data}")

        parsed = parse_tilda_data(data)

        # Название листа = название турнира из заявки
        sheet_name = parsed.get("turnir", "").strip()
        if not sheet_name:
            raise ValueError("Поле 'turnir' пустое — невозможно определить лист таблицы")

        sheet = get_sheet(sheet_name)

        # Находим первую свободную строку
        next_row = find_next_empty_row(sheet)
        logger.info(f"Записываем в лист '{sheet_name}', строка {next_row}")

        # Номер строки в колонке A (порядковый номер = row - 4)
        sheet.update_cell(next_row, col_letter_to_index("A"), next_row - 4)

        # Заполняем только нужные колонки — формулы в остальных не трогаем
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

    except ValueError as e:
        logger.warning(f"Ошибка валидации: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Ошибка при обработке заявки: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
