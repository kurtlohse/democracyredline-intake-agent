from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any
import hashlib
import re
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
    source_role: str
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


def strip_html(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"(?i)<\s*br\s*/?\s*>", " ", text)
    text = re.sub(r"(?i)<\s*/\s*p\s*>", " ", text)
    text = re.sub(r"(?i)<\s*/\s*div\s*>", " ", text)
    text = re.sub(r"(?i)<\s*/\s*li\s*>", " ", text)

    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def trim_summary(text: str, max_len: int = 500) -> str:
    if len(text) <= max_len:
        return text
    trimmed = text[:max_len].rsplit(" ", 1)[0].strip()
    return trimmed + "..."


def extract_summary(entry: Any) -> str:
    raw = ""

    for attr in ("summary", "description"):
        value = getattr(entry, attr, None)
        if value:
            raw = clean_text(value)
            break

    if not raw:
        content = getattr(entry, "content", None)
        if content and isinstance(content, list):
            first = content[0] if content else {}
            if isinstance(first, dict) and first.get("value"):
                raw = clean_text(first["value"])

    cleaned = strip_html(raw)
    cleaned = trim_summary(cleaned, max_len=500)
    return cleaned


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def fetch_single_feed(source: dict[str, Any], timeout: int = 20) -> list[FeedItem]:
    url = clean_text(source.get("url"))
    if not url:
        return []

    source_name = clean_text(source.get("name", "Unknown Source"))
    source_tier = clean_text(source.get("tier", "Tier 2"))
    source_role = clean_text(source.get("role", "evidence"))
    source_reliability = float(source.get("reliability", 0.80))

    headers = {
        "User-Agent": "DemocracyRedlineIntakeBot/2.0 (+https://democracyredline.com)",
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
                source_role=source_role,
                source_reliability=source_reliability,
                title=title,
                summary=summary,
                link=link,
            )
        )

    return items


def dedupe_items(items: list[FeedItem]) -> list[FeedItem]:
    seen: set[str] = set()
    out: list[FeedItem] = []
    for item in items:
        key = stable_hash(normalize_link(item.link))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def sort_items(items: list[FeedItem]) -> list[FeedItem]:
    return sorted(items, key=lambda x: (x.published_at, x.source_reliability), reverse=True)


def fetch_all_feeds() -> list[FeedItem]:
    sources = load_sources()
    all_items: list[FeedItem] = []

    for source in sources:
        if not bool(source.get("enabled", True)):
            continue
        items = fetch_single_feed(source)
        all_items.extend(items)
        time.sleep(0.30)

    all_items = dedupe_items(all_items)
    all_items = sort_items(all_items)
    return all_items
