from __future__ import annotations

import csv
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from fetch_feeds import fetch_all_feeds
from sheets_writer import append_rows_to_sheet

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_CSV = OUTPUT_DIR / "monthly_intake.csv"
RULES_PATH = ROOT / "config" / "agent_priority_rules.yaml"

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
    "editor_priority",
    "needs_manual_review",
    "evidence_strength",
    "month_assigned",
    "include_in_report",
    "score_impact_candidate",
    "report_section",
    "duplicate_cluster",
    "final_disposition",
    "reviewed_by",
    "reviewed_on",
    "notes",
]

DEFAULT_RULES: dict[str, Any] = {
    "top_priority_sources": [
        "reuters",
        "associated press",
        "ap news",
        "agence france-presse",
        "afp",
        "bbc",
        "bbc news",
        "financial times",
        "washington post",
        "new york times",
        "propublica",
    ],
    "high_priority_sources": [
        "lawfare",
        "brennan center",
        "bright line watch",
        "v-dem",
        "international idea",
        "wall street journal",
        "wsj",
        "bloomberg",
        "npr",
        "pbs",
        "politico",
        "time",
        "the guardian",
        "cnn",
        "abc news",
        "nbc news",
        "cbs news",
    ],
    "watch_organizations": [
        "aclu",
        "brennan center",
        "democracy forward",
        "legal defense fund",
        "ldf",
        "pacific legal foundation",
        "lawfare",
        "campaign legal center",
        "protect democracy",
        "states united democracy center",
    ],
    "watch_institutions": [
        "fbi",
        "doj",
        "department of justice",
        "dhs",
        "department of homeland security",
        "ice",
        "pentagon",
        "national guard",
        "irs",
        "federal election commission",
        "election board",
        "supreme court",
        "department of defense",
    ],
    "watch_targets": [
        "journalist",
        "reporter",
        "judge",
        "election official",
        "candidate",
        "lawmaker",
        "opponent",
        "governor",
        "mayor",
        "protester",
        "activist",
        "civil servant",
        "inspector general",
        "watchdog",
    ],
    "urgent_trigger_groups": {
        "court_defiance": [
            "court order",
            "defied court",
            "ignored court",
            "ignored ruling",
            "contempt",
            "injunction",
            "stay order",
            "refused to comply",
            "judicial order",
            "violated order",
        ],
        "election_interference": [
            "election certification",
            "refused to certify",
            "ballot access",
            "voter purge",
            "elector",
            "voting machine",
            "peaceful transfer",
            "election official",
            "vote count",
            "voter roll",
            "gerrymander",
            "redistricting",
        ],
        "weaponized_justice": [
            "release file",
            "investigate opponent",
            "retaliatory investigation",
            "political prosecution",
            "selective prosecution",
            "targeting rival",
            "fbi file",
            "doj file",
            "cease-and-desist",
            "smear",
            "grand jury against opponent",
            "prosecution of opponent",
        ],
        "press_intimidation": [
            "journalist arrested",
            "reporter arrested",
            "newsroom raid",
            "subpoenaed reporter",
            "press access revoked",
            "media threat",
            "press freedom",
            "detained journalist",
        ],
        "coercive_state_power": [
            "national guard",
            "military deployment",
            "domestic troop use",
            "insurrection act",
            "martial law",
            "mass raids",
            "federal agents",
            "riot control",
            "paramilitary",
            "surveillance powers",
        ],
        "due_process": [
            "habeas",
            "due process",
            "indefinite detention",
            "detention without hearing",
            "deportation",
            "rendition",
            "without hearing",
            "without counsel",
        ],
        "institutional_capture": [
            "inspector general fired",
            "watchdog removed",
            "civil service purge",
            "installed loyalist",
            "stacking agency",
            "independent agency",
            "purge",
            "watchdog ousted",
        ],
        "corruption": [
            "bribery",
            "kickback",
            "pay to play",
            "self-dealing",
            "conflict of interest",
            "ethics violation",
            "corruption probe",
        ],
    },
}


def load_rules() -> dict[str, Any]:
    if RULES_PATH.exists():
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            user_rules = yaml.safe_load(f) or {}
        merged = DEFAULT_RULES.copy()
        merged.update(user_rules)
        return merged
    return DEFAULT_RULES


RULES = load_rules()


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


def evidence_strength_from_reliability(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "Weak"
    if score >= 0.90:
        return "Strong"
    if score >= 0.80:
        return "Moderate"
    return "Weak"


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


def source_priority(source_name: str) -> str:
    name = normalize(source_name)
    if any(s in name for s in RULES.get("top_priority_sources", [])):
        return "Top"
    if any(s in name for s in RULES.get("high_priority_sources", [])):
        return "High"
    if any(s in name for s in RULES.get("watch_organizations", [])):
        return "Watch"
    return "Standard"


def watch_entity_hits(text: str) -> list[str]:
    hits: list[str] = []
    for term in RULES.get("watch_institutions", []):
        if term in text:
            hits.append(term)
    for term in RULES.get("watch_targets", []):
        if term in text:
            hits.append(term)
    seen = set()
    ordered = []
    for h in hits:
        if h not in seen:
            seen.add(h)
            ordered.append(h)
    return ordered


def suggest_primary_signal(text: str) -> str:
    signal_rules: list[tuple[str, list[str]]] = [
        ("Election interference", ["election", "ballot", "vote count", "voter roll", "certification", "elector", "polling place", "voting rights", "peaceful transfer"]),
        ("Court defiance", ["court order", "defied court", "defy court", "ignored ruling", "ignored court", "judicial order", "contempt", "injunction", "supreme court order"]),
        ("Weaponized justice", ["prosecution of opponent", "targeting rival", "retaliatory investigation", "political prosecution", "weaponized justice", "selective prosecution", "federal charges against opponent", "release file", "fbi file", "doj file"]),
        ("Political violence", ["violence", "violent threat", "intimidation", "armed group", "assassination", "attack on", "threatened judge", "threatened election worker", "mob"]),
        ("Civil liberties erosion", ["due process", "habeas", "detention", "deportation", "protest rights", "free speech", "speech restrictions", "assembly", "surveillance", "civil liberties", "rights violation"]),
        ("Press intimidation", ["press access", "newsroom", "journalist", "media threat", "press freedom", "libel threat", "reporter barred", "reporter arrested", "journalist arrested"]),
        ("Information control", ["censorship", "propaganda", "disinformation office", "content moderation pressure", "book ban", "information control", "state media"]),
        ("Corruption", ["corruption", "bribery", "kickback", "self-dealing", "conflict of interest", "embezzlement", "ethics violation", "pay to play"]),
        ("Militarization", ["military deployment", "troops", "national guard", "martial law", "insurrection act", "military used domestically", "paramilitary"]),
        ("Institutional capture", ["purge", "loyalist", "stacking agency", "capture of", "inspectors general", "independent agency", "civil service purge", "watchdog removed", "watchdog ousted"]),
        ("Executive overreach", ["executive order", "emergency power", "sweeping authority", "expanded power", "presidential power", "unitary executive"]),
    ]
    for label, keywords in signal_rules:
        if any(keyword in text for keyword in keywords):
            return label
    return ""


def suggest_category(text: str, primary_signal: str) -> str:
    category_rules: list[tuple[str, list[str]]] = [
        ("Election Integrity & Peaceful Transfer", ["election", "ballot", "vote count", "certification", "elector", "voter roll", "peaceful transfer"]),
        ("Rule of Law & Court Compliance", ["court order", "injunction", "ignored ruling", "defied court", "judicial order", "supreme court", "contempt"]),
        ("Habeas Corpus & Due Process", ["habeas", "due process", "detention", "deportation", "rendition", "indefinite detention"]),
        ("Coercive State Power & Policing Norms", ["police", "federal agents", "riot control", "military deployment", "national guard", "insurrection act", "surveillance", "raids"]),
        ("Political Targeting / Weaponization of Justice", ["retaliatory investigation", "targeting rival", "political prosecution", "weaponized justice", "selective prosecution", "opponent charged", "release file", "fbi file", "doj file"]),
        ("Press Freedom & Information Control", ["journalist", "newsroom", "press access", "media threat", "censorship", "information control", "state media", "reporter arrested"]),
        ("Civil Society & Associational Freedom", ["protest", "assembly", "civil society", "nonprofit restrictions", "campus protest", "union organizing", "speech restrictions"]),
        ("Institutional Checks & Anti-Corruption", ["inspector general", "ethics violation", "conflict of interest", "corruption", "bribery", "self-dealing", "watchdog removed", "oversight blocked"]),
        ("Military & Intelligence Neutrality", ["military loyalty", "politicized intelligence", "domestic troop use", "armed forces", "intelligence agency", "chain of command"]),
    ]
    for category, keywords in category_rules:
        if any(keyword in text for keyword in keywords):
            return category
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


def trigger_group_hits(text: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for group, keywords in RULES.get("urgent_trigger_groups", {}).items():
        matched = [keyword for keyword in keywords if keyword in text]
        if matched:
            hits[group] = matched
    return hits


def compute_escalation_score(
    source_tier: str,
    confidence: str,
    src_priority: str,
    primary_signal: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: list[str],
) -> int:
    score = 0

    if src_priority == "Top":
        score += 3
    elif src_priority == "High":
        score += 2
    elif src_priority == "Watch":
        score += 1

    if source_tier == "Tier 1":
        score += 2
    elif source_tier == "Tier 2":
        score += 1

    if confidence == "High":
        score += 2
    elif confidence == "Medium":
        score += 1

    if primary_signal:
        score += 2

    if len(trigger_hits) >= 2:
        score += 3
    elif len(trigger_hits) == 1:
        score += 1

    if len(entity_hits) >= 2:
        score += 2
    elif len(entity_hits) == 1:
        score += 1

    if "weaponized_justice" in trigger_hits:
        score += 2
    if "court_defiance" in trigger_hits:
        score += 2
    if "election_interference" in trigger_hits:
        score += 2
    if "press_intimidation" in trigger_hits:
        score += 2

    return score


def suggest_score_impact_candidate(
    escalation_score: int,
    primary_signal: str,
) -> str:
    if escalation_score >= 9:
        return "Likely"
    if escalation_score >= 5 and primary_signal:
        return "Possible"
    if primary_signal:
        return "Possible"
    return "Unlikely"


def suggest_editor_priority(
    escalation_score: int,
    score_impact_candidate: str,
) -> str:
    if escalation_score >= 10 or score_impact_candidate == "Likely":
        return "Urgent"
    if escalation_score >= 7:
        return "High"
    if escalation_score >= 4:
        return "Medium"
    return "Low"


def suggest_needs_manual_review(
    category: str,
    primary_signal: str,
    confidence: str,
    include_in_report: str,
    score_impact_candidate: str,
    editor_priority: str,
) -> str:
    if not category or not primary_signal:
        return "Yes"
    if editor_priority in {"Urgent", "High"}:
        return "Yes"
    if score_impact_candidate in {"Likely", "Possible"}:
        return "Yes"
    if confidence == "High" and include_in_report == "Maybe":
        return "Yes"
    return "No"


def make_duplicate_cluster_seed(item: dict[str, str]) -> str:
    title = normalize(item.get("title", ""))
    words = re.findall(r"[a-z0-9]+", title)
    stopwords = {
        "the", "a", "an", "and", "or", "of", "to", "in", "for", "on", "with",
        "after", "over", "under", "at", "by", "from", "into", "about", "amid",
        "trump", "us", "u", "s",
    }
    filtered = [w for w in words if w not in stopwords and len(w) > 2]
    if not filtered:
        return ""
    common = filtered[:3]
    return "CLUSTER-" + "-".join(common).upper()


def auto_notes(
    src_priority: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: list[str],
    primary_signal: str,
    score_impact_candidate: str,
    escalation_score: int,
) -> str:
    parts = []
    if src_priority in {"Top", "High", "Watch"}:
        parts.append(f"source={src_priority}")
    if primary_signal:
        parts.append(f"signal={primary_signal}")
    if score_impact_candidate:
        parts.append(f"impact={score_impact_candidate}")
    parts.append(f"score={escalation_score}")
    if trigger_hits:
        parts.append("triggers=" + ", ".join(list(trigger_hits.keys())[:3]))
    if entity_hits:
        parts.append("watch_hits=" + ", ".join(entity_hits[:3]))
    return "AUTO: " + " | ".join(parts)


def build_row(item: Any) -> dict[str, str]:
    published_at = clean_text(getattr(item, "published_at", ""))
    source_reliability = clean_text(getattr(item, "source_reliability", ""))
    source_tier = clean_text(getattr(item, "source_tier", ""))
    source_name = clean_text(getattr(item, "source_name", ""))
    text = combined_text(item)

    src_priority = source_priority(source_name)
    entity_hits = watch_entity_hits(text)
    trigger_hits = trigger_group_hits(text)
    primary_signal = suggest_primary_signal(text)
    category = suggest_category(text, primary_signal)
    confidence = confidence_from_reliability(source_reliability)
    evidence_strength = evidence_strength_from_reliability(source_reliability)

    escalation_score = compute_escalation_score(
        source_tier=source_tier,
        confidence=confidence,
        src_priority=src_priority,
        primary_signal=primary_signal,
        trigger_hits=trigger_hits,
        entity_hits=entity_hits,
    )
    score_impact_candidate = suggest_score_impact_candidate(
        escalation_score=escalation_score,
        primary_signal=primary_signal,
    )
    editor_priority = suggest_editor_priority(
        escalation_score=escalation_score,
        score_impact_candidate=score_impact_candidate,
    )
    include_in_report = "Maybe"
    needs_manual_review = suggest_needs_manual_review(
        category=category,
        primary_signal=primary_signal,
        confidence=confidence,
        include_in_report=include_in_report,
        score_impact_candidate=score_impact_candidate,
        editor_priority=editor_priority,
    )

    row = {
        "date_collected": datetime.now(timezone.utc).isoformat(),
        "published_at": published_at,
        "source_name": source_name,
        "source_tier": source_tier,
        "title": clean_text(getattr(item, "title", "")),
        "summary": clean_text(getattr(item, "summary", "")),
        "link": clean_text(getattr(item, "link", "")),
        "source_reliability": source_reliability,
        "review_status": "New",
        "democracy_redline_category": category,
        "primary_signal": primary_signal,
        "secondary_signal": "",
        "confidence": confidence,
        "editor_priority": editor_priority,
        "needs_manual_review": needs_manual_review,
        "evidence_strength": evidence_strength,
        "month_assigned": month_from_published(published_at),
        "include_in_report": include_in_report,
        "score_impact_candidate": score_impact_candidate,
        "report_section": "",
        "duplicate_cluster": "",
        "final_disposition": "",
        "reviewed_by": "",
        "reviewed_on": "",
        "notes": auto_notes(src_priority, trigger_hits, entity_hits, primary_signal, score_impact_candidate, escalation_score),
    }

    row["duplicate_cluster"] = make_duplicate_cluster_seed(row)
    return row


def refine_duplicate_clusters(rows: list[dict[str, str]]) -> None:
    seeds = [row.get("duplicate_cluster", "") for row in rows if row.get("duplicate_cluster", "")]
    counts = Counter(seeds)
    for row in rows:
        seed = row.get("duplicate_cluster", "")
        if not seed:
            continue
        if counts.get(seed, 0) < 2:
            row["duplicate_cluster"] = ""
        else:
            row["duplicate_cluster"] = seed


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
    refine_duplicate_clusters(rows)
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
