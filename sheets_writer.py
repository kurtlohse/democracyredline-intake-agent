from __future__ import annotations

import csv
import os
from pathlib import Path

import gspread


def append_csv_to_sheet(csv_path: str, worksheet_name: str = "Intake") -> int:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        print("No GOOGLE_SHEETS_SPREADSHEET_ID set; skipping Sheets write.")
        return 0

    gc = gspread.service_account()
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)

    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"CSV file not found: {csv_path}")
        return 0

    with open(csv_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) <= 1:
        print("No data rows found in CSV; nothing to append.")
        return 0

    header, data_rows = rows[0], rows[1:]

    existing_header = ws.row_values(1)
    if not existing_header:
        ws.append_row(header)

    for row in data_rows:
        ws.append_row(row, value_input_option="RAW")

    print(f"Appended {len(data_rows)} rows to worksheet '{worksheet_name}'.")
    return len(data_rows)
