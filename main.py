from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Settings:
    openai_api_key: str
    openai_model: str
    google_sheets_enabled: bool
    google_sheet_id: str | None
    google_worksheet_name: str
    google_service_account_json: dict[str, Any] | None
    max_items_per_run: int
    lookback_days: int
    min_credible_sources: int
    output_csv_path: Path


def load_settings() -> Settings:
    load_dotenv(ROOT / ".env")

    svc_json_raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    svc_json = json.loads(svc_json_raw) if svc_json_raw else None

    return Settings(
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
        google_sheets_enabled=os.getenv("GOOGLE_SHEETS_ENABLED", "false").lower() == "true",
        google_sheet_id=os.getenv("GOOGLE_SHEET_ID") or None,
        google_worksheet_name=os.getenv("GOOGLE_WORKSHEET_NAME", "Monthly Intake"),
        google_service_account_json=svc_json,
        max_items_per_run=int(os.getenv("MAX_ITEMS_PER_RUN", "20")),
        lookback_days=int(os.getenv("LOOKBACK_DAYS", "10")),
        min_credible_sources=int(os.getenv("MIN_CREDIBLE_SOURCES", "2")),
        output_csv_path=ROOT / os.getenv("OUTPUT_CSV_PATH", "output/monthly_intake.csv"),
    )


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)
