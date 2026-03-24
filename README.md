from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Iterable

from fetch_feeds import FeedItem


def _normalize(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def cluster_items(items: Iterable[FeedItem]) -> list[dict]:
    buckets: dict[str, list[FeedItem]] = defaultdict(list)
    for item in items:
        basis = _normalize(item.title)[:120]
        key = hashlib.md5(basis.encode("utf-8")).hexdigest()
        buckets[key].append(item)

    clusters = []
    for _, grouped in buckets.items():
        grouped = sorted(grouped, key=lambda x: (x.published_at or "", x.source_reliability), reverse=True)
        clusters.append(
            {
                "cluster_title": grouped[0].title,
                "representative_summary": grouped[0].summary,
                "published_at": grouped[0].published_at,
                "sources": [g.to_dict() for g in grouped],
                "links": [g.link for g in grouped if g.link],
                "corroboration_count": len(grouped),
                "best_source_tier": sorted([g.source_tier for g in grouped])[0] if grouped else "Tier 3",
            }
        )
    return sorted(clusters, key=lambda c: (c["published_at"] or ""), reverse=True)
