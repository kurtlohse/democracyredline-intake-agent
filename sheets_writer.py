from __future__ import annotations

import os
from typing import Sequence

import google.auth
import gspread


def get_gspread_client() -> gspread.Client:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    return gspread.authorize(credentials)


def get_or_create_worksheet(
    spreadsheet: gspread.Spreadsheet,
    worksheet_name: str,
    rows: int = 2000,
    cols: int = 40,
) -> gspread.Worksheet:
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=worksheet_name, rows=rows, cols=cols)


def ensure_header(
    worksheet: gspread.Worksheet,
    headers: Sequence[str],
) -> None:
    existing_header = worksheet.row_values(1)
    if existing_header != list(headers):
        worksheet.update("A1", [list(headers)])


def get_existing_links(worksheet: gspread.Worksheet, headers: Sequence[str]) -> set[str]:
    try:
        link_index = list(headers).index("link") + 1
    except ValueError:
        return set()

    col_values = worksheet.col_values(link_index)
    if len(col_values) <= 1:
        return set()

    return {value.strip() for value in col_values[1:] if value.strip()}


def get_existing_titles(
    worksheet: gspread.Worksheet,
    headers: Sequence[str],
    max_rows: int = 500,
) -> list[str]:
    try:
        title_index = list(headers).index("title") + 1
    except ValueError:
        return []

    col_values = worksheet.col_values(title_index)
    if len(col_values) <= 1:
        return []

    titles = [value.strip() for value in col_values[1:] if value.strip()]
    if max_rows > 0:
        titles = titles[-max_rows:]
    return titles


def get_existing_sheet_values(
    worksheet_name: str = "Intake",
    headers: Sequence[str] | None = None,
    max_title_rows: int = 500,
) -> tuple[set[str], list[str]]:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        return set(), []

    if headers is None:
        return set(), []

    try:
        gc = get_gspread_client()
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
    except Exception as e:
        print(f"Could not read existing sheet values from '{worksheet_name}': {e}")
        return set(), []

    existing_links = get_existing_links(worksheet, headers)
    existing_titles = get_existing_titles(worksheet, headers, max_rows=max_title_rows)
    return existing_links, existing_titles


def append_rows_to_sheet(
    rows: list[dict[str, str]],
    worksheet_name: str = "Intake",
    headers: Sequence[str] | None = None,
) -> int:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        print("No GOOGLE_SHEETS_SPREADSHEET_ID set; skipping Sheets write.")
        return 0

    if not rows:
        print("No rows supplied; skipping Sheets write.")
        return 0

    if headers is None:
        headers = list(rows[0].keys())

    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = get_or_create_worksheet(spreadsheet, worksheet_name)

    ensure_header(worksheet, headers)
    existing_links = get_existing_links(worksheet, headers)

    new_rows: list[list[str]] = []
    seen_this_batch: set[str] = set()

    for row in rows:
        link = row.get("link", "").strip()
        if not link:
            continue
        if link in existing_links:
            continue
        if link in seen_this_batch:
            continue

        seen_this_batch.add(link)
        new_rows.append([row.get(header, "") for header in headers])

    if not new_rows:
        print("No new rows to append after dedupe.")
        return 0

    worksheet.append_rows(new_rows, value_input_option="RAW")
    return len(new_rows)
