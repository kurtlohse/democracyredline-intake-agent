from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
import hashlib
import time

import feedparser
import requests
import yaml


ROOT = Path(__file__).resolve().parent
SOURCES_PATH = ROOT / "config" / "sources.yaml"


@dataclass
class FeedItem:
    published_at: str
    source_name: str
    source_tier: str
    source_reliability: float
    title: str
    summary: str
    link: str


def load_sources() -> list[dict[str, Any]]:
    if not SOURCES_PATH.exists():
        raise FileNotFoundError(f"Missing source config: {SOURCES_PATH}")
    with open(SOURCES_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("sources", [])


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_link(link: str) -> str:
    link = clean_text(link)
    if not link:
        return ""
    return link.split("#")[0].strip()


def parse_published(entry: Any) -> str:
    # Try feedparser date fields first
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    # Fall back to raw strings
    for attr in ("published", "updated", "pubDate"):
        value = getattr(entry, attr, None)
        if value:
            try:
                dt = parsedate_to_datetime(value)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                continue

    return datetime.now(timezone.utc).isoformat()


def extract_summary(entry: Any) -> str:
    # Prefer summary-like fields
    for attr in ("summary", "description"):
        value = getattr(entry, attr, None)
        if value:
            return clean_text(value)

    # Some feeds put content into entry.content
    content = getattr(entry, "content", None)
    if content and isinstance(content, list):
        first = content[0] if content else {}
        if isinstance(first, dict) and first.get("value"):
            return clean_text(first["value"])

    return ""


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def fetch_single_feed(source: dict[str, Any], timeout: int = 20) -> list[FeedItem]:
    url = clean_text(source.get("url"))
    if not url:
        return []

    source_name = clean_text(source.get("name", "Unknown Source"))
    source_tier = clean_text(source.get("tier", "Tier 2"))
    source_reliability = float(source.get("reliability", 0.80))

    headers = {
        "User-Agent": "DemocracyRedlineIntakeBot/1.0 (+https://democracyredline.com)",
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
    }

    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
    except Exception as e:
        print(f"Feed fetch failed for {source_name} ({url}): {e}")
        return []

    parsed = feedparser.parse(response.content)
    items: list[FeedItem] = []

    for entry in parsed.entries:
        link = normalize_link(getattr(entry, "link", ""))
        title = clean_text(getattr(entry, "title", ""))
        summary = extract_summary(entry)
        published_at = parse_published(entry)

        if not link or not title:
            continue

        items.append(
            FeedItem(
                published_at=published_at,
                source_name=source_name,
                source_tier=source_tier,
                source_reliability=source_reliability,
                title=title,
                summary=summary,
                link=link,
            )
        )

    return items


def dedupe_items(items: list[FeedItem]) -> list[FeedItem]:
    seen: set[str] = set()
    deduped: list[FeedItem] = []

    for item in items:
        key = stable_hash(normalize_link(item.link))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return deduped


def sort_items(items: list[FeedItem]) -> list[FeedItem]:
    def key(item: FeedItem) -> tuple[str, float]:
        return (item.published_at, item.source_reliability)

    return sorted(items, key=key, reverse=True)


def fetch_all_feeds() -> list[FeedItem]:
    sources = load_sources()
    all_items: list[FeedItem] = []

    for source in sources:
        enabled = bool(source.get("enabled", True))
        if not enabled:
            continue

        items = fetch_single_feed(source)
        all_items.extend(items)

        # Small pause to be polite and reduce burstiness
        time.sleep(0.35)

    all_items = dedupe_items(all_items)
    all_items = sort_items(all_items)
    return all_items
