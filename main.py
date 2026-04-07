import base64
import io
import json
import os
import re
import tempfile
from datetime import datetime

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from contracts import generate_all_contracts

app = FastAPI()

APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "")

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def parse_person_list(text: str) -> list:
    """Parse textarea person list into rows: [fio, dob, phone].

    Expected format — 2 lines per person:
      Line 1: FIO DD.MM.YYYY  (date may be DD.MM YYYY or attached without space)
      Line 2: phone number
    Also handles legacy comma-separated: FIO, DD.MM.YYYY, phone
    """
    date_re = re.compile(r"(\d{2}\.\d{2}[\. ]\d{4})\s*")
    phone_re = re.compile(r"^[\+\d][\d\s\-\(\)]{5,}")

    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    rows = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # Legacy: comma-separated on one line
        if "," in line:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 3:
                rows.append([parts[0], parts[1], parts[2]])
            elif len(parts) == 2:
                rows.append([parts[0], parts[1], ""])
            else:
                rows.append([line, "", ""])
            i += 1
            continue

        # Skip orphan phone-only lines
        if phone_re.match(line):
            i += 1
            continue

        # Extract date from end of line
        dm = date_re.search(line)
        if dm:
            raw_date = dm.group(1)
            # Normalise: DD.MM YYYY -> DD.MM.YYYY
            date_str = re.sub(r"(\d{2}\.\d{2}) (\d{4})", r"\1.\2", raw_date)
            fio = line[: dm.start()].strip()
        else:
            date_str = ""
            fio = line

        # Next line: phone?
        phone = ""
        if i + 1 < len(lines) and phone_re.match(lines[i + 1]):
            phone = lines[i + 1]
            i += 2
        else:
            i += 1

        rows.append([fio, date_str, phone])

    return rows


async def upload_to_drive(
    content_bytes: bytes,
    filename: str,
    folder_id: str = "",
) -> str:
    """Upload a file to Google Drive via Apps Script proxy. Returns shareable URL."""
    b64 = base64.b64encode(content_bytes).decode()
    payload = {
        "action": "upload_file",
        "filename": filename,
        "content_base64": b64,
        "folder_id": folder_id,
    }
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.post(APPS_SCRIPT_URL, json=payload)
    data = resp.json()
    if data.get("status") != "ok":
        raise RuntimeError(f"Drive upload failed: {data}")
    return data.get("url", "")


# ─────────────────────────────────────────────
# Existing webhook — Tilda form → Google Sheets
# ─────────────────────────────────────────────

@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.body()
        try:
            data = json.loads(body)
        except Exception:
            from urllib.parse import parse_qs
            parsed = parse_qs(body.decode("utf-8", errors="replace"))
            data = {k: v[0] for k, v in parsed.items()}

        # Pass the full payload to Apps Script for Sheets processing
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                APPS_SCRIPT_URL,
                json={"action": "tilda_webhook", "data": data},
            )
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


# ─────────────────────────────────────────────
# New endpoint — generate contracts
# ─────────────────────────────────────────────

@app.post("/generate-contracts")
async def generate_contracts(request: Request):
    """
    Called by Google Apps Script checkbox trigger.

    Expected JSON payload:
    {
      "team": "ГАУ ДО СО СШОР №1, Киселев Дмитрий Владимирович",
      "arrival": "10.04.2026 9:00:00",
      "departure": "12.04.2026 14:00:00",
      "nights_accommodation": 2.0,
      "nights_food": 2.0,
      "children": 14,
      "coaches": 1,
      "parents": 2,
      "price_food": 1200,
      "price_accommodation": 1950,
      "cost_food": 50150,
      "cost_accommodation": 66300,
      "cost_transfer": 16750,
      "arrival_transport": "09.04.2026 22:43 ж/д вокзал Самара, поезд 147",
      "departure_transport": "12.04.2026 ж/д вокзал Самара",
      "contact_person": "Киселев Дмитрий Владимирович",
      "phone": "8 (922) 106-58-83",
      "email": "dmitrii-loskov@mail.ru",
      "tournament_name": "Стремление Д2013-2014",
      "folder_id": "optional_drive_folder_id",
      "meal_schedule": {
        "10.04.2026": ["обед", "ужин"],
        "12.04.2026": ["завтрак", "обед"]
      }
    }

    Returns:
    {
      "status": "ok",
      "links": {
        "transport_contract": "https://drive.google.com/...",
        "transport_appendix": "https://drive.google.com/...",
        "food_contract": "https://drive.google.com/...",
        "food_appendix": "https://drive.google.com/...",
        "accommodation_contract": "https://drive.google.com/..."
      }
    }
    """
    try:
        payload = await request.json()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    try:
        docs = generate_all_contracts(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Generation error: {e}")

    folder_id = payload.get("folder_id", "")
    team_short = (
        payload.get("contact_person", "Команда")
        .split()[-1]  # last name only for filename
    )

    file_map = {
        "transport_contract":    (f"Договор_перевозки_{team_short}.docx",    "transport_contract"),
        "transport_appendix":    (f"Приложение_перевозка_{team_short}.xlsx",  "transport_appendix"),
        "food_contract":         (f"Договор_питания_{team_short}.docx",       "food_contract"),
        "food_appendix":         (f"Приложение_питание_{team_short}.xlsx",    "food_appendix"),
        "accommodation_contract": (f"Договор_проживания_{team_short}.docx",   "accommodation_contract"),
    }

    links = {}
    errors = []
    for key, (filename, doc_key) in file_map.items():
        try:
            url = await upload_to_drive(docs[doc_key], filename, folder_id)
            links[key] = url
        except Exception as e:
            errors.append(f"{key}: {e}")
            links[key] = ""

    if errors:
        return JSONResponse(
            {"status": "partial", "links": links, "errors": errors}
        )
    return JSONResponse({"status": "ok", "links": links})


@app.get("/")
async def health():
    return {"status": "ok"}
