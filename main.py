from __future__ import annotations

import csv
import os
import re
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

CATEGORY_OPTIONS = [
    "Rule of Law & Court Compliance",
    "Habeas Corpus & Due Process",
    "Coercive State Power & Policing Norms",
    "Political Targeting / Weaponization of Justice",
    "Election Integrity & Peaceful Transfer",
    "Press Freedom & Information Control",
    "Civil Society & Associational Freedom",
    "Institutional Checks & Anti-Corruption",
    "Military & Intelligence Neutrality",
]

PRIMARY_SIGNAL_OPTIONS = [
    "Executive overreach",
    "Court defiance",
    "Election interference",
    "Political violence",
    "Civil liberties erosion",
    "Press intimidation",
    "Corruption",
    "Militarization",
    "Weaponized justice",
    "Institutional capture",
    "Information control",
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


def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def combined_text(item: Any) -> str:
    title = clean_text(getattr(item, "title", ""))
    summary = clean_text(getattr(item, "summary", ""))
    source_name = clean_text(getattr(item, "source_name", ""))
    return normalize(f"{title} {summary} {source_name}")


def suggest_primary_signal(text: str) -> str:
    signal_rules: list[tuple[str, list[str]]] = [
        (
            "Election interference",
            [
                "election",
                "ballot",
                "vote count",
                "voter roll",
                "certification",
                "elector",
                "polling place",
                "voting rights",
                "peaceful transfer",
            ],
        ),
        (
            "Court defiance",
            [
                "court order",
                "defied court",
                "defy court",
                "ignored ruling",
                "ignored court",
                "judicial order",
                "contempt",
                "injunction",
                "supreme court order",
            ],
        ),
        (
            "Weaponized justice",
            [
                "prosecution of opponent",
                "targeting rival",
                "retaliatory investigation",
                "political prosecution",
                "weaponized justice",
                "selective prosecution",
                "federal charges against opponent",
            ],
        ),
        (
            "Political violence",
            [
                "violence",
                "violent threat",
                "intimidation",
                "armed group",
                "assassination",
                "attack on",
                "threatened judge",
                "threatened election worker",
                "mob",
            ],
        ),
        (
            "Civil liberties erosion",
            [
                "due process",
                "habeas",
                "detention",
                "deportation",
                "protest rights",
                "free speech",
                "speech restrictions",
                "assembly",
                "surveillance",
                "civil liberties",
                "rights violation",
            ],
        ),
        (
            "Press intimidation",
            [
                "press access",
                "newsroom",
                "journalist",
                "media threat",
                "press freedom",
                "libel threat",
                "reporter barred",
                "reporter arrested",
            ],
        ),
        (
            "Information control",
            [
                "censorship",
                "propaganda",
                "disinformation office",
                "content moderation pressure",
                "book ban",
                "information control",
                "state media",
            ],
        ),
        (
            "Corruption",
            [
                "corruption",
                "bribery",
                "kickback",
                "self-dealing",
                "conflict of interest",
                "embezzlement",
                "ethics violation",
                "pay to play",
            ],
        ),
        (
            "Militarization",
            [
                "military deployment",
                "troops",
                "national guard",
                "martial law",
                "insurrection act",
                "military used domestically",
                "paramilitary",
            ],
        ),
        (
            "Institutional capture",
            [
                "purge",
                "loyalist",
                "stacking agency",
                "capture of",
                "inspectors general",
                "independent agency",
                "civil service purge",
                "watchdog removed",
            ],
        ),
        (
            "Executive overreach",
            [
                "executive order",
                "emergency power",
                "sweeping authority",
                "expanded power",
                "presidential power",
                "unitary executive",
            ],
        ),
    ]

    for label, keywords in signal_rules:
        if any(keyword in text for keyword in keywords):
            return label

    return ""


def suggest_category(text: str, primary_signal: str) -> str:
    category_rules: list[tuple[str, list[str]]] = [
        (
            "Election Integrity & Peaceful Transfer",
            [
                "election",
                "ballot",
                "vote count",
                "certification",
                "elector",
                "voter roll",
                "peaceful transfer",
            ],
        ),
        (
            "Rule of Law & Court Compliance",
            [
                "court order",
                "injunction",
                "ignored ruling",
                "defied court",
                "judicial order",
                "supreme court",
                "contempt",
            ],
        ),
        (
            "Habeas Corpus & Due Process",
            [
                "habeas",
                "due process",
                "detention",
                "deportation",
                "rendition",
                "indefinite detention",
            ],
        ),
        (
            "Coercive State Power & Policing Norms",
            [
                "police",
                "federal agents",
                "riot control",
                "military deployment",
                "national guard",
                "insurrection act",
                "surveillance",
                "raids",
            ],
        ),
        (
            "Political Targeting / Weaponization of Justice",
            [
                "retaliatory investigation",
                "targeting rival",
                "political prosecution",
                "weaponized justice",
                "selective prosecution",
                "opponent charged",
            ],
        ),
        (
            "Press Freedom & Information Control",
            [
                "journalist",
                "newsroom",
                "press access",
                "media threat",
                "censorship",
                "information control",
                "state media",
                "reporter arrested",
            ],
        ),
        (
            "Civil Society & Associational Freedom",
            [
                "protest",
                "assembly",
                "civil society",
                "nonprofit restrictions",
                "campus protest",
                "union organizing",
                "speech restrictions",
            ],
        ),
        (
            "Institutional Checks & Anti-Corruption",
            [
                "inspector general",
                "ethics violation",
                "conflict of interest",
                "corruption",
                "bribery",
                "self-dealing",
                "watchdog removed",
                "oversight blocked",
            ],
        ),
        (
            "Military & Intelligence Neutrality",
            [
                "military loyalty",
                "politicized intelligence",
                "domestic troop use",
                "armed forces",
                "intelligence agency",
                "chain of command",
            ],
        ),
    ]

    for category, keywords in category_rules:
        if any(keyword in text for keyword in keywords):
            return category

    # Fallbacks from primary signal
    fallback_map = {
        "Election interference": "Election Integrity & Peaceful Transfer",
        "Court defiance": "Rule of Law & Court Compliance",
        "Weaponized justice": "Political Targeting / Weaponization of Justice",
        "Political violence": "Civil Society & Associational Freedom",
        "Civil liberties erosion": "Habeas Corpus & Due Process",
        "Press intimidation": "Press Freedom & Information Control",
        "Information control": "Press Freedom & Information Control",
        "Corruption": "Institutional Checks & Anti-Corruption",
        "Militarization": "Military & Intelligence Neutrality",
        "Institutional capture": "Institutional Checks & Anti-Corruption",
        "Executive overreach": "Rule of Law & Court Compliance",
    }
    return fallback_map.get(primary_signal, "")


def build_row(item: Any) -> dict[str, str]:
    published_at = clean_text(getattr(item, "published_at", ""))
    source_reliability = clean_text(getattr(item, "source_reliability", ""))
    text = combined_text(item)
    primary_signal = suggest_primary_signal(text)
    category = suggest_category(text, primary_signal)

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
        "democracy_redline_category": category,
        "primary_signal": primary_signal,
        "secondary_signal": "",
        "confidence": confidence_from_reliability(source_reliability),
        "month_assigned": month_from_published(published_at),
        "include_in_report": "Maybe",
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
        new_count = append_rows_to_sheet(rows, worksheet_name="Intake", headers=HEADERS)
        if new_count == 0:
            print("No new rows to append after dedupe.")
        print(f"Appended {new_count} new rows to worksheet 'Intake'.")
    except Exception as e:
        print(f"Google Sheets append failed: {e}")


if __name__ == "__main__":
    main()
