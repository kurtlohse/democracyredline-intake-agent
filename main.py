from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fetch_feeds import fetch_all_feeds

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_CSV = OUTPUT_DIR / "monthly_intake.csv"


def iso_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "10"))
    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "50"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    items = fetch_all_feeds()
    filtered = []

    for item in items:
        published_dt = iso_to_dt(getattr(item, "published_at", None))
        if published_dt and published_dt < cutoff:
            continue
        filtered.append(item)

    filtered = filtered[:max_items]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date_collected",
            "published_at",
            "source_name",
            "source_tier",
            "title",
            "summary",
            "link",
            "source_reliability",
        ])

        now_str = datetime.now(timezone.utc).isoformat()
        for item in filtered:
            writer.writerow([
                now_str,
                getattr(item, "published_at", ""),
                getattr(item, "source_name", ""),
                getattr(item, "source_tier", ""),
                getattr(item, "title", ""),
                getattr(item, "summary", ""),
                getattr(item, "link", ""),
                getattr(item, "source_reliability", ""),
            ])

    print(f"Wrote {len(filtered)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
