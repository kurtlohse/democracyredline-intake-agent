from __future__ import annotations

import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from sheets_writer import get_gspread_client
from main import (
    HEADERS,
    DERIVED_FIELDS,
    MANUAL_FIELDS,
    build_row_from_values,
    clean_text,
    validate_rows,
)

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
BACKUP_CSV = OUTPUT_DIR / "intake_backup_before_rescore.csv"
RESCORED_CSV = OUTPUT_DIR / "intake_rescored_preview.csv"


def get_intake_records(worksheet_name: str = "Intake") -> tuple[object, list[dict[str, str]]]:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("GOOGLE_SHEETS_SPREADSHEET_ID is not set.")

    gc = get_gspread_client()
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)

    records = worksheet.get_all_records(expected_headers=HEADERS)
    normalized_records: list[dict[str, str]] = []
    for record in records:
        row = {header: clean_text(record.get(header, "")) for header in HEADERS}
        normalized_records.append(row)

    return worksheet, normalized_records


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def repair_legacy_note_damage(row: dict[str, str]) -> dict[str, str]:
    repaired = dict(row)
    report_section = clean_text(repaired.get("report_section", ""))
    notes = clean_text(repaired.get("notes", ""))

    if not notes and report_section.startswith("AUTO:"):
        repaired["notes"] = report_section
        repaired["report_section"] = ""

    if clean_text(repaired.get("report_section", "")).startswith("AUTO:"):
        repaired["report_section"] = ""

    return repaired


def rescore_rows(existing_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rescored: list[dict[str, str]] = []

    for old in existing_rows:
        repaired_old = repair_legacy_note_damage(old)

        new_row = build_row_from_values(
            title=clean_text(repaired_old.get("title", "")),
            summary=clean_text(repaired_old.get("summary", "")),
            source_name=clean_text(repaired_old.get("source_name", "")),
            source_tier=clean_text(repaired_old.get("source_tier", "")),
            source_role=clean_text(repaired_old.get("source_role", "evidence")),
            source_reliability=clean_text(repaired_old.get("source_reliability", "")),
            published_at=clean_text(repaired_old.get("published_at", "")),
            link=clean_text(repaired_old.get("link", "")),
            existing_row=repaired_old,
        )

        for field in MANUAL_FIELDS:
            if field in repaired_old:
                new_row[field] = clean_text(repaired_old.get(field, ""))

        if clean_text(repaired_old.get("date_collected", "")):
            new_row["date_collected"] = clean_text(repaired_old["date_collected"])

        if not clean_text(new_row.get("notes", "")) and clean_text(new_row.get("report_section", "")).startswith("AUTO:"):
            new_row["notes"] = clean_text(new_row["report_section"])
            new_row["report_section"] = ""

        rescored.append(new_row)

    return rescored


def column_index_to_a1(col_index: int) -> str:
    result = ""
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def update_sheet_in_place(worksheet, rows: list[dict[str, str]]) -> None:
    all_values: list[list[str]] = [HEADERS]
    for row in rows:
        all_values.append([row.get(header, "") for header in HEADERS])

    end_col = column_index_to_a1(len(HEADERS))
    end_row = len(all_values)

    worksheet.update(
        values=all_values,
        range_name=f"A1:{end_col}{end_row}",
    )


def print_change_summary(before: list[dict[str, str]], after: list[dict[str, str]]) -> None:
    changed_rows = 0
    changed_cells = 0

    for old, new in zip(before, after):
        row_changed = False
        for field in DERIVED_FIELDS.union({"notes", "report_section"}):
            if clean_text(old.get(field, "")) != clean_text(new.get(field, "")):
                changed_cells += 1
                row_changed = True
        if row_changed:
            changed_rows += 1

    print(f"Rows with derived-field changes: {changed_rows}")
    print(f"Derived cells changed: {changed_cells}")


def main() -> None:
    worksheet, existing_rows = get_intake_records("Intake")
    if not existing_rows:
        print("No Intake rows found to rescore.")
        return

    validate_rows(existing_rows)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(BACKUP_CSV, existing_rows)
    print(f"Wrote backup CSV to {BACKUP_CSV}")

    rescored_rows = rescore_rows(existing_rows)
    validate_rows(rescored_rows)
    write_csv(RESCORED_CSV, rescored_rows)
    print(f"Wrote rescored preview CSV to {RESCORED_CSV}")

    print_change_summary(existing_rows, rescored_rows)

    dry_run = os.getenv("DRY_RUN", "true").lower() != "false"
    if dry_run:
        print("DRY_RUN is enabled. No sheet updates were written.")
        print("Set DRY_RUN=false to write rescored rows back to the Intake sheet.")
        return

    update_sheet_in_place(worksheet, rescored_rows)
    print(
        f"Updated worksheet 'Intake' with {len(rescored_rows)} rescored rows at "
        f"{datetime.now(timezone.utc).isoformat()}"
    )


if __name__ == "__main__":
    main()
