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
    "source_role",
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
    "admission_decision",
    "event_type",
    "category_fit",
    "event_definiteness",
    "democratic_consequence",
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


def load_rules() -> dict[str, Any]:
    if not RULES_PATH.exists():
        raise FileNotFoundError(f"Missing rules file: {RULES_PATH}")
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


RULES = load_rules()


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize(text: str) -> str:
    text = clean_text(text).lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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
    if score >= 0.92:
        return "High"
    if score >= 0.84:
        return "Medium"
    return "Low"


def evidence_strength_from_reliability(value: Any) -> str:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return "Weak"
    if score >= 0.92:
        return "Strong"
    if score >= 0.84:
        return "Moderate"
    return "Weak"


def combined_text(item: Any) -> str:
    title = clean_text(getattr(item, "title", ""))
    summary = clean_text(getattr(item, "summary", ""))
    source_name = clean_text(getattr(item, "source_name", ""))
    return normalize(f"{title} {summary} {source_name}")


def compile_phrase_pattern(term: str) -> re.Pattern[str]:
    term = normalize(term)
    parts = [re.escape(p) for p in term.split() if p]
    if not parts:
        return re.compile(r"$^")
    if len(parts) == 1:
        return re.compile(rf"\b{parts[0]}\b", re.IGNORECASE)
    joined = r"\s+".join(parts)
    return re.compile(rf"(?<!\w){joined}(?!\w)", re.IGNORECASE)


COMPILED_EXCLUSIONS = [compile_phrase_pattern(t) for t in RULES.get("exclude_if_title_or_summary_contains", [])]
COMPILED_SOURCE_PRIORITY = {
    "top_sources": [compile_phrase_pattern(t) for t in RULES.get("top_priority_sources", [])],
    "high_sources": [compile_phrase_pattern(t) for t in RULES.get("high_priority_sources", [])],
    "watch_sources": [compile_phrase_pattern(t) for t in RULES.get("watch_sources", [])],
}
COMPILED_TRIGGER_GROUPS = {
    group: [compile_phrase_pattern(t) for t in terms]
    for group, terms in RULES.get("trigger_groups", {}).items()
}


def matches_any(text: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(p.search(text) for p in patterns)


def matched_terms(text: str, raw_terms: list[str]) -> list[str]:
    hits = []
    for term in raw_terms:
        if compile_phrase_pattern(term).search(text):
            hits.append(term)
    return hits


def source_priority(source_name: str, source_role: str) -> str:
    name = normalize(source_name)
    if matches_any(name, COMPILED_SOURCE_PRIORITY["top_sources"]):
        return "Top"
    if matches_any(name, COMPILED_SOURCE_PRIORITY["high_sources"]):
        return "High"
    if source_role in {"watchdog", "investigative"} or matches_any(name, COMPILED_SOURCE_PRIORITY["watch_sources"]):
        return "Watch"
    return "Standard"


def trigger_group_hits(text: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for group, raw_terms in RULES.get("trigger_groups", {}).items():
        matched = matched_terms(text, raw_terms)
        if matched:
            hits[group] = matched
    return hits


def watch_entity_hits(text: str) -> dict[str, list[str]]:
    institutions = matched_terms(text, RULES.get("watch_institutions", []))
    targets = matched_terms(text, RULES.get("watch_targets", []))
    return {
        "institutions": institutions,
        "targets": targets,
    }


def exclusion_hits(text: str) -> list[str]:
    hits = []
    for term in RULES.get("exclude_if_title_or_summary_contains", []):
        if compile_phrase_pattern(term).search(text):
            hits.append(term)
    return hits


def suggest_primary_signal(text: str) -> str:
    mapping = [
        ("Election interference", "election_interference"),
        ("Court defiance", "court_defiance"),
        ("Weaponized justice", "weaponized_justice"),
        ("Press intimidation", "press_intimidation"),
        ("Civil liberties erosion", "due_process"),
        ("Militarization", "coercive_state_power"),
        ("Institutional capture", "institutional_capture"),
        ("Corruption", "corruption"),
    ]
    hits = trigger_group_hits(text)
    for label, group in mapping:
        if group in hits:
            return label
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("signal_fallbacks", {}).get("executive_overreach", [])):
        return "Executive overreach"
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
        if any(compile_phrase_pattern(k).search(text) for k in keywords):
            return category
    fallback_map = {
        "Election interference": "Election Integrity & Peaceful Transfer",
        "Court defiance": "Rule of Law & Court Compliance",
        "Weaponized justice": "Political Targeting / Weaponization of Justice",
        "Civil liberties erosion": "Habeas Corpus & Due Process",
        "Press intimidation": "Press Freedom & Information Control",
        "Corruption": "Institutional Checks & Anti-Corruption",
        "Militarization": "Coercive State Power & Policing Norms",
        "Institutional capture": "Institutional Checks & Anti-Corruption",
        "Executive overreach": "Rule of Law & Court Compliance",
    }
    return fallback_map.get(primary_signal, "")


def classify_event_type(text: str, exclusion_terms: list[str]) -> str:
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("supreme_court_ruling", [])):
        return "Supreme Court Ruling"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("court_ruling", [])):
        return "Court Ruling"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("court_filing", [])):
        return "Court Filing"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("arrest_detention", [])):
        return "Arrest / Detention"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("executive_or_agency_action", [])):
        return "Executive Order / Agency Action"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("media_targeting", [])):
        return "Media Restriction / Journalist Targeting"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("election_action", [])):
        return "Election Administration Action"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("watchdog_removal", [])):
        return "Watchdog / Oversight Removal"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("military_security", [])):
        return "Military / Security Deployment"
    if exclusion_terms:
        return "Commentary / Preview"
    if any(compile_phrase_pattern(t).search(text) for t in RULES.get("event_type_patterns", {}).get("developing", [])):
        return "Developing / Unconfirmed"
    return "General Context"


def classify_event_definiteness(event_type: str) -> str:
    if event_type in {"Court Ruling", "Supreme Court Ruling", "Arrest / Detention", "Executive Order / Agency Action", "Election Administration Action", "Media Restriction / Journalist Targeting", "Watchdog / Oversight Removal", "Military / Security Deployment"}:
        return "Confirmed Action"
    if event_type == "Court Filing":
        return "Filed Case"
    if event_type == "Developing / Unconfirmed":
        return "Developing / Unconfirmed"
    if event_type == "Commentary / Preview":
        return "Commentary / Preview"
    return "General Context"


def classify_category_fit(category: str, primary_signal: str, trigger_hits: dict[str, list[str]]) -> str:
    if category and (primary_signal or len(trigger_hits) >= 1):
        return "Direct"
    if category or primary_signal:
        return "Partial"
    return "Weak"


def classify_democratic_consequence(event_type: str, category_fit: str, trigger_hits: dict[str, list[str]]) -> str:
    if category_fit == "Direct" and event_type in {
        "Court Ruling",
        "Supreme Court Ruling",
        "Arrest / Detention",
        "Executive Order / Agency Action",
        "Election Administration Action",
        "Media Restriction / Journalist Targeting",
        "Watchdog / Oversight Removal",
        "Military / Security Deployment",
    }:
        return "Immediate"
    if category_fit == "Direct" and event_type == "Court Filing":
        return "Material"
    if category_fit in {"Direct", "Partial"} and event_type == "Developing / Unconfirmed":
        return "Possible"
    if category_fit == "Partial" and trigger_hits:
        return "Possible"
    return "Remote"


def admission_decision(
    source_role: str,
    category_fit: str,
    event_definiteness: str,
    democratic_consequence: str,
    exclusion_terms: list[str],
    trigger_hits: dict[str, list[str]],
) -> str:
    if event_definiteness == "Commentary / Preview":
        return "Reject"
    if category_fit == "Direct" and event_definiteness in {"Confirmed Action", "Filed Case"} and democratic_consequence in {"Immediate", "Material"}:
        return "Main Intake"
    if source_role in {"watchdog", "investigative"} and category_fit in {"Direct", "Partial"} and event_definiteness in {"Confirmed Action", "Filed Case", "Developing / Unconfirmed"}:
        return "Watchlist"
    if category_fit in {"Direct", "Partial"} and democratic_consequence == "Possible":
        return "Watchlist"
    if len(trigger_hits) >= 2 and category_fit != "Weak":
        return "Watchlist"
    return "Reject"


def compute_escalation_score(
    source_tier: str,
    confidence: str,
    src_priority: str,
    primary_signal: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
    event_definiteness: str,
) -> int:
    score = 0

    if event_definiteness in {"Confirmed Action", "Filed Case"}:
        score += 4
    elif event_definiteness == "Developing / Unconfirmed":
        score += 1

    if primary_signal:
        score += 2

    trigger_group_count = len(trigger_hits)
    if trigger_group_count >= 3:
        score += 4
    elif trigger_group_count == 2:
        score += 3
    elif trigger_group_count == 1:
        score += 1

    entity_count = len(entity_hits.get("institutions", [])) + len(entity_hits.get("targets", []))
    if entity_count >= 3:
        score += 3
    elif entity_count == 2:
        score += 2
    elif entity_count == 1:
        score += 1

    if src_priority == "Top":
        score += 1
    elif src_priority == "High":
        score += 1

    if confidence == "High":
        score += 2
    elif confidence == "Medium":
        score += 1

    if source_tier == "Tier 1":
        score += 1

    if "weaponized_justice" in trigger_hits:
        score += 2
    if "court_defiance" in trigger_hits:
        score += 2
    if "election_interference" in trigger_hits:
        score += 2

    return score


def suggest_score_impact_candidate(escalation_score: int, admission: str, primary_signal: str) -> str:
    if admission == "Reject":
        return "Unlikely"
    if escalation_score >= 10:
        return "Likely"
    if escalation_score >= 5 and primary_signal:
        return "Possible"
    if admission == "Watchlist":
        return "Possible"
    return "Unlikely"


def suggest_editor_priority(
    escalation_score: int,
    score_impact_candidate: str,
    trigger_hits: dict[str, list[str]],
) -> str:
    strong_trigger_groups = {"weaponized_justice", "court_defiance", "election_interference"}
    strong_count = sum(1 for k in trigger_hits if k in strong_trigger_groups)

    if score_impact_candidate == "Unlikely":
        if strong_count >= 2:
            return "Medium"
        return "Low"

    if escalation_score >= 12 and strong_count >= 2:
        return "Urgent"
    if escalation_score >= 8:
        return "High"
    if escalation_score >= 4:
        return "Medium"
    return "Low"


def suggest_needs_manual_review(
    admission: str,
    category: str,
    primary_signal: str,
    score_impact_candidate: str,
    editor_priority: str,
) -> str:
    if admission == "Reject":
        return "No"
    if not category or not primary_signal:
        return "Yes"
    if editor_priority in {"Urgent", "High"}:
        return "Yes"
    if score_impact_candidate in {"Likely", "Possible"}:
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
    return "CLUSTER-" + "-".join(filtered[:3]).upper()


def auto_notes(
    src_priority: str,
    admission: str,
    event_type: str,
    category_fit: str,
    democratic_consequence: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
    escalation_score: int,
) -> str:
    parts = [
        f"source={src_priority}",
        f"admission={admission}",
        f"event={event_type}",
        f"fit={category_fit}",
        f"consequence={democratic_consequence}",
        f"score={escalation_score}",
    ]
    if trigger_hits:
        parts.append("triggers=" + ", ".join(list(trigger_hits.keys())[:3]))
    wh = entity_hits.get("institutions", []) + entity_hits.get("targets", [])
    if wh:
        parts.append("watch_hits=" + ", ".join(wh[:3]))
    return "AUTO: " + " | ".join(parts)


def build_row(item: Any) -> dict[str, str]:
    published_at = clean_text(getattr(item, "published_at", ""))
    source_reliability = clean_text(getattr(item, "source_reliability", ""))
    source_tier = clean_text(getattr(item, "source_tier", ""))
    source_name = clean_text(getattr(item, "source_name", ""))
    source_role = clean_text(getattr(item, "source_role", "evidence"))
    text = combined_text(item)

    exclusion_terms = exclusion_hits(text)
    trigger_hits = trigger_group_hits(text)
    entity_hits = watch_entity_hits(text)
    primary_signal = suggest_primary_signal(text)
    category = suggest_category(text, primary_signal)
    confidence = confidence_from_reliability(source_reliability)
    evidence_strength = evidence_strength_from_reliability(source_reliability)
    src_priority = source_priority(source_name, source_role)

    event_type = classify_event_type(text, exclusion_terms)
    event_definiteness = classify_event_definiteness(event_type)
    category_fit = classify_category_fit(category, primary_signal, trigger_hits)
    democratic_consequence = classify_democratic_consequence(event_type, category_fit, trigger_hits)
    admission = admission_decision(
        source_role=source_role,
        category_fit=category_fit,
        event_definiteness=event_definiteness,
        democratic_consequence=democratic_consequence,
        exclusion_terms=exclusion_terms,
        trigger_hits=trigger_hits,
    )
    escalation_score = compute_escalation_score(
        source_tier=source_tier,
        confidence=confidence,
        src_priority=src_priority,
        primary_signal=primary_signal,
        trigger_hits=trigger_hits,
        entity_hits=entity_hits,
        event_definiteness=event_definiteness,
    )
    score_impact_candidate = suggest_score_impact_candidate(
        escalation_score=escalation_score,
        admission=admission,
        primary_signal=primary_signal,
    )
    editor_priority = suggest_editor_priority(
        escalation_score=escalation_score,
        score_impact_candidate=score_impact_candidate,
        trigger_hits=trigger_hits,
    )
    include_in_report = "Maybe" if admission != "Reject" else "No"
    needs_manual_review = suggest_needs_manual_review(
        admission=admission,
        category=category,
        primary_signal=primary_signal,
        score_impact_candidate=score_impact_candidate,
        editor_priority=editor_priority,
    )

    row = {
        "date_collected": datetime.now(timezone.utc).isoformat(),
        "published_at": published_at,
        "source_name": source_name,
        "source_tier": source_tier,
        "source_role": source_role,
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
        "admission_decision": admission,
        "event_type": event_type,
        "category_fit": category_fit,
        "event_definiteness": event_definiteness,
        "democratic_consequence": democratic_consequence,
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
        "notes": auto_notes(
            src_priority=src_priority,
            admission=admission,
            event_type=event_type,
            category_fit=category_fit,
            democratic_consequence=democratic_consequence,
            trigger_hits=trigger_hits,
            entity_hits=entity_hits,
            escalation_score=escalation_score,
        ),
    }
    row["duplicate_cluster"] = make_duplicate_cluster_seed(row)
    return row


def refine_duplicate_clusters(rows: list[dict[str, str]]) -> None:
    seeds = [r.get("duplicate_cluster", "") for r in rows if r.get("duplicate_cluster", "")]
    counts = Counter(seeds)
    for row in rows:
        seed = row.get("duplicate_cluster", "")
        if not seed:
            continue
        if counts.get(seed, 0) < 2:
            row["duplicate_cluster"] = ""


def dedupe_rows_by_link(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for row in rows:
        link = row.get("link", "").strip()
        if not link or link in seen:
            continue
        seen.add(link)
        out.append(row)
    return out


def write_csv(rows: list[dict[str, str]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    lookback_days = int(os.getenv("LOOKBACK_DAYS", "10"))
    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "75"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    items = fetch_all_feeds()
    rows: list[dict[str, str]] = []

    for item in items:
        published_dt = iso_to_dt(getattr(item, "published_at", None))
        if published_dt and published_dt < cutoff:
            continue
        row = build_row(item)
        if row["admission_decision"] == "Reject":
            continue
        rows.append(row)

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
