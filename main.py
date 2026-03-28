from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fetch_feeds import fetch_all_feeds
from sheets_writer import append_rows_to_sheet

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_CSV = OUTPUT_DIR / "monthly_intake.csv"

HEADERS = [
    "date_collected",
    "published_at",
    "source_name",
    "source_tier",
    "title",
    "summary",
    "link",
    "source_reliability",
    "review_status",
    "democracy_redline_category",
    "primary_signal",
    "secondary_signal",
    "confidence",
    "month_assigned",
    "include_in_report",
    "notes",
]


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def month_from_published(value: str | None) -> str:
    dt = iso_to_dt(value)
    if dt is None:
        now = datetime.now(timezone.utc)
        return f"{now.year:04d}-{now.month:02d}"
    return f"{dt.year:04d}-{dt.month:02d}"


def confidence_from_reliability(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "Low"

    if score >= 0.90:
        return "High"
    if score >= 0.80:
        return "Medium"
    return "Low"


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_row(item: Any) -> dict[str, str]:
    published_at = clean_text(getattr(item, "published_at", ""))
    source_reliability = clean_text(getattr(item, "source_reliability", ""))

    return {
        "date_collected": datetime.now(timezone.utc).isoformat(),
        "published_at": published_at,
        "source_name": clean_text(getattr(item, "source_name", "")),
        "source_tier": clean_text(getattr(item, "source_tier", "")),
        "title": clean_text(getattr(item, "title", "")),
        "summary": clean_text(getattr(item, "summary", "")),
        "link": clean_text(getattr(item, "link", "")),
        "source_reliability": source_reliability,
        "review_status": "New",
        "democracy_redline_category": "",
        "primary_signal": "",
        "secondary_signal": "",
        "confidence": confidence_from_reliability(source_reliability),
        "month_assigned": month_from_published(published_at),
        "include_in_report": "",
        "notes": "",
    }


def dedupe_rows_by_link(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []

    for row in rows:
        link = row.get("link", "").strip()
        if not link:
            continue
        if link in seen:
            continue
        seen.add(link)
        deduped.append(row)

    return deduped


def write_csv(rows: list[dict[str, str]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "10"))
    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "50"))

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    items = fetch_all_feeds()
    rows: list[dict[str, str]] = []

    for item in items:
        published_dt = iso_to_dt(getattr(item, "published_at", None))
        if published_dt and published_dt < cutoff:
            continue
        rows.append(build_row(item))

    rows = dedupe_rows_by_link(rows)
    rows = rows[:max_items]

    write_csv(rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")

    try:
        appended = append_rows_to_sheet(rows, worksheet_name="Intake", headers=HEADERS)
        print(f"Appended {appended} new rows to worksheet 'Intake'.")
    except Exception as e:
        print(f"Google Sheets append failed: {e}")


if __name__ == "__main__":
    main()
