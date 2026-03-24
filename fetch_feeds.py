from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List
import email.utils

import feedparser
import yaml


ROOT = Path(__file__).resolve().parent
SOURCES_FILE = ROOT / "sources.yaml"


@dataclass
class FeedItem:
    title: str
    summary: str
    link: str
    published_at: str
    source_name: str
    source_tier: str
    source_reliability: float


def _to_iso(entry) -> str:
    parsed = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if parsed:
        dt = datetime(*parsed[:6], tzinfo=timezone.utc)
        return dt.isoformat()
    published = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if published:
        try:
            dt = email.utils.parsedate_to_datetime(published)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            pass
    return ""


def load_sources() -> list[dict]:
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("feeds", [])


def fetch_all_feeds() -> List[FeedItem]:
    items: List[FeedItem] = []
    for source in load_sources():
        url = source.get("url")
        if not url:
            continue

        parsed = feedparser.parse(url)
        for entry in parsed.entries[:20]:
            items.append(
                FeedItem(
                    title=getattr(entry, "title", "") or "",
                    summary=getattr(entry, "summary", "") or getattr(entry, "description", "") or "",
                    link=getattr(entry, "link", "") or "",
                    published_at=_to_iso(entry),
                    source_name=source.get("name", ""),
                    source_tier=source.get("source_tier", "Tier 2"),
                    source_reliability=float(source.get("reliability", 0.8)),
                )
            )
    return items
