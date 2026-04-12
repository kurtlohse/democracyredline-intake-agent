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
from sheets_writer import append_rows_to_sheet, get_existing_sheet_values

ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
OUTPUT_CSV = OUTPUT_DIR / "monthly_intake.csv"
RULES_PATH = ROOT / "config" / "agent_priority_rules.yaml"
RUNTIME_SETTINGS_PATH = ROOT / "config" / "runtime_settings.yaml"

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
    "threat_cluster",
    "cluster_status",
    "cluster_escalation_score",
    "governing_function",
    "oversight_failure_flag",
    "report_section",
    "duplicate_cluster",
    "final_disposition",
    "reviewed_by",
    "reviewed_on",
    "notes",
]

CONCRETE_EVENT_TYPES = {
    "Court Filing",
    "Court Ruling",
    "Supreme Court Ruling",
    "Arrest / Detention",
    "Executive Order / Agency Action",
    "Election Administration Action",
    "Media Restriction / Journalist Targeting",
    "Watchdog / Oversight Removal",
    "Military / Security Deployment",
}

REPEAT_PRONE_WATCHDOGS = {
    "campaign legal center",
    "protect democracy",
    "democracy forward",
    "democracy docket",
    "aclu",
}


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


RULES = load_yaml(RULES_PATH)
RUNTIME_SETTINGS = load_yaml(RUNTIME_SETTINGS_PATH)


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
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def dt_to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


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
    joined = r"[\s\-/,:;()]+".join(parts)
    return re.compile(rf"(?<!\w){joined}(?!\w)", re.IGNORECASE)


def matched_terms(text: str, raw_terms: list[str]) -> list[str]:
    hits: list[str] = []
    for term in raw_terms:
        if compile_phrase_pattern(term).search(text):
            hits.append(term)
    return hits


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
    return matched_terms(text, RULES.get("exclude_if_title_or_summary_contains", []))


def source_priority(source_name: str, source_role: str) -> str:
    name = normalize(source_name)
    if matched_terms(name, RULES.get("top_priority_sources", [])):
        return "Top"
    if matched_terms(name, RULES.get("high_priority_sources", [])):
        return "High"
    if source_role in {"watchdog", "investigative"} or matched_terms(name, RULES.get("watch_sources", [])):
        return "Watch"
    return "Standard"


def strong_trigger_count(trigger_hits: dict[str, list[str]]) -> int:
    strong_groups = {
        "court_defiance",
        "election_interference",
        "weaponized_justice",
        "press_intimidation",
        "coercive_state_power",
        "due_process",
        "institutional_capture",
        "oversight_failure",
    }
    return sum(1 for k in trigger_hits if k in strong_groups)


def is_repeat_prone_watchdog(source_name: str, source_role: str) -> bool:
    if source_role not in {"watchdog", "investigative"}:
        return False
    source_name_norm = normalize(source_name)
    return any(name in source_name_norm for name in REPEAT_PRONE_WATCHDOGS)


def title_stem(title: str) -> str:
    words = re.findall(r"[a-z0-9]+", normalize(title))
    stopwords = {
        "the", "a", "an", "and", "or", "of", "to", "in", "for", "on", "with",
        "after", "over", "under", "at", "by", "from", "into", "about", "amid",
        "trump", "us", "u", "s",
    }
    filtered = [w for w in words if w not in stopwords and len(w) > 2]
    return " ".join(filtered[:8])


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
        ("Executive overreach", "oversight_failure"),
    ]
    hits = trigger_group_hits(text)
    for label, group in mapping:
        if group in hits:
            return label
    if any(
        compile_phrase_pattern(t).search(text)
        for t in RULES.get("signal_fallbacks", {}).get("executive_overreach", [])
    ):
        return "Executive overreach"
    return ""


def suggest_category(text: str, primary_signal: str) -> str:
    category_rules: list[tuple[str, list[str]]] = [
        (
            "Election Integrity & Peaceful Transfer",
            ["election", "ballot", "vote count", "certification", "elector", "voter roll", "peaceful transfer"],
        ),
        (
            "Rule of Law & Court Compliance",
            ["court order", "injunction", "ignored ruling", "defied court", "judicial order", "supreme court", "contempt"],
        ),
        (
            "Habeas Corpus & Due Process",
            ["habeas", "due process", "detention", "deportation", "rendition", "indefinite detention"],
        ),
        (
            "Coercive State Power & Policing Norms",
            ["federal agents", "military deployment", "national guard", "insurrection act", "surveillance", "raids", "troops deployed"],
        ),
        (
            "Political Targeting / Weaponization of Justice",
            ["retaliatory investigation", "political prosecution", "selective prosecution", "justice department targeted", "grand jury against opponent"],
        ),
        (
            "Press Freedom & Information Control",
            ["journalist", "newsroom", "press access", "media threat", "reporter arrested", "journalist barred"],
        ),
        (
            "Civil Society & Associational Freedom",
            ["protest", "assembly", "civil society", "speech restrictions"],
        ),
        (
            "Institutional Checks & Anti-Corruption",
            ["inspector general", "ethics violation", "conflict of interest", "corruption", "bribery", "watchdog removed", "oversight blocked", "war powers", "subpoena defied"],
        ),
        (
            "Military & Intelligence Neutrality",
            ["military loyalty", "politicized intelligence", "domestic troop use", "armed forces", "intelligence agency", "chain of command"],
        ),
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
        "Executive overreach": "Institutional Checks & Anti-Corruption",
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
    if event_type in {
        "Court Ruling",
        "Supreme Court Ruling",
        "Arrest / Detention",
        "Executive Order / Agency Action",
        "Election Administration Action",
        "Media Restriction / Journalist Targeting",
        "Watchdog / Oversight Removal",
        "Military / Security Deployment",
    }:
        return "Confirmed Action"
    if event_type == "Court Filing":
        return "Filed Case"
    if event_type == "Developing / Unconfirmed":
        return "Developing / Unconfirmed"
    if event_type == "Commentary / Preview":
        return "Commentary / Preview"
    return "General Context"


def classify_category_fit(
    category: str,
    primary_signal: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
) -> str:
    trigger_count = len(trigger_hits)
    entity_count = len(entity_hits.get("institutions", [])) + len(entity_hits.get("targets", []))

    if category and (primary_signal or trigger_count >= 1):
        return "Direct"
    if category and entity_count >= 2:
        return "Direct"
    if category or primary_signal or trigger_count >= 1:
        return "Partial"
    return "Weak"


def classify_democratic_consequence(
    event_type: str,
    category_fit: str,
    trigger_hits: dict[str, list[str]],
    primary_signal: str,
) -> str:
    trigger_count = len(trigger_hits)
    strong_count = strong_trigger_count(trigger_hits)

    if category_fit == "Direct" and event_type in CONCRETE_EVENT_TYPES - {"Court Filing"}:
        return "Immediate"

    if category_fit == "Direct" and event_type == "Court Filing":
        return "Material"

    if event_type == "Developing / Unconfirmed" and (trigger_count >= 2 or strong_count >= 1):
        return "Possible"

    if category_fit == "Partial" and primary_signal and (trigger_count >= 1 or strong_count >= 1):
        return "Possible"

    return "Remote"


def freshness_bonus(published_at: str) -> int:
    dt = iso_to_dt(published_at)
    if dt is None:
        return 0

    age = datetime.now(timezone.utc) - dt
    high_days = int(RUNTIME_SETTINGS.get("freshness_days_high", 3))
    medium_days = int(RUNTIME_SETTINGS.get("freshness_days_medium", 7))

    if age <= timedelta(days=high_days):
        return 3
    if age <= timedelta(days=medium_days):
        return 2
    return 0


def admission_decision(
    source_name: str,
    source_role: str,
    category_fit: str,
    event_type: str,
    event_definiteness: str,
    democratic_consequence: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
    primary_signal: str,
    published_at: str,
) -> str:
    trigger_count = len(trigger_hits)
    strong_count = strong_trigger_count(trigger_hits)
    entity_count = len(entity_hits.get("institutions", [])) + len(entity_hits.get("targets", []))
    repeat_prone_watchdog = is_repeat_prone_watchdog(source_name, source_role)
    freshness = freshness_bonus(published_at)

    if event_definiteness == "Commentary / Preview":
        return "Reject"

    if (
        category_fit == "Direct"
        and event_type in CONCRETE_EVENT_TYPES
        and event_definiteness in {"Confirmed Action", "Filed Case"}
        and democratic_consequence in {"Immediate", "Material"}
    ):
        return "Main Intake"

    if (
        source_role == "evidence"
        and category_fit in {"Direct", "Partial"}
        and (
            event_type in CONCRETE_EVENT_TYPES
            or (primary_signal and strong_count >= 1 and category_fit == "Direct")
        )
        and (trigger_count >= 1 or strong_count >= 1 or primary_signal)
    ):
        return "Watchlist"

    if (
        source_role in {"watchdog", "investigative"}
        and not repeat_prone_watchdog
        and event_type in CONCRETE_EVENT_TYPES
        and category_fit in {"Direct", "Partial"}
        and (trigger_count >= 1 or strong_count >= 1 or (primary_signal and entity_count >= 2))
    ):
        return "Watchlist"

    if (
        repeat_prone_watchdog
        and event_type in {"Court Ruling", "Supreme Court Ruling", "Arrest / Detention", "Executive Order / Agency Action"}
        and category_fit == "Direct"
        and strong_count >= 1
    ):
        return "Watchlist"

    if (
        repeat_prone_watchdog
        and event_type == "Court Filing"
        and category_fit == "Direct"
        and (strong_count >= 2 or (freshness >= 2 and entity_count >= 2))
    ):
        return "Watchlist"

    return "Reject"


def determine_governing_function(
    category: str,
    trigger_hits: dict[str, list[str]],
    oversight_failure_flag: str,
) -> str:
    functions = set()

    if category == "Institutional Checks & Anti-Corruption" or oversight_failure_flag == "Yes":
        functions.add("Legislative Oversight")
    if category == "Rule of Law & Court Compliance":
        functions.add("Judicial Enforcement")
    if category == "Habeas Corpus & Due Process":
        functions.add("Civil Liberties Protection")
    if category == "Election Integrity & Peaceful Transfer":
        functions.add("Election Integrity")
    if category == "Press Freedom & Information Control":
        functions.add("Press Independence")
    if category == "Coercive State Power & Policing Norms":
        functions.add("Lawful Force")
    if category == "Political Targeting / Weaponization of Justice":
        functions.add("Executive Constraint")
    if category == "Military & Intelligence Neutrality":
        functions.add("Lawful Force")

    if "oversight_failure" in trigger_hits:
        functions.add("Legislative Oversight")
    if "court_defiance" in trigger_hits:
        functions.add("Judicial Enforcement")
    if "election_interference" in trigger_hits:
        functions.add("Election Integrity")
    if "press_intimidation" in trigger_hits:
        functions.add("Press Independence")
    if "due_process" in trigger_hits:
        functions.add("Civil Liberties Protection")
    if "coercive_state_power" in trigger_hits:
        functions.add("Lawful Force")

    if not functions:
        return "Multiple" if len(trigger_hits) >= 2 else "Executive Constraint"
    if len(functions) == 1:
        return next(iter(functions))
    return "Multiple"


def determine_oversight_failure_flag(trigger_hits: dict[str, list[str]]) -> str:
    return "Yes" if "oversight_failure" in trigger_hits else "No"


def determine_threat_cluster(
    text: str,
    primary_signal: str,
    category: str,
    trigger_hits: dict[str, list[str]],
    oversight_failure_flag: str,
) -> str:
    if "weaponized_justice" in trigger_hits:
        return "DOJ_TARGETING_OPPONENTS_2026"
    if "court_defiance" in trigger_hits and "due_process" in trigger_hits:
        return "COURT_DEFIANCE_DUE_PROCESS_2026"
    if "election_interference" in trigger_hits:
        return "ELECTION_ADMIN_INTEGRITY_2026"
    if oversight_failure_flag == "Yes" and ("coercive_state_power" in trigger_hits or "war powers" in text):
        return "WAR_POWERS_OVERSIGHT_2026"
    if "press_intimidation" in trigger_hits:
        return "PRESS_INTIMIDATION_2026"
    if "institutional_capture" in trigger_hits:
        return "WATCHDOG_CAPTURE_2026"
    if primary_signal == "Executive overreach" and category == "Institutional Checks & Anti-Corruption":
        return "EXECUTIVE_OVERSIGHT_EROSION_2026"
    return ""


def compute_cluster_escalation_score(
    source_role: str,
    source_tier: str,
    confidence: str,
    event_type: str,
    oversight_failure_flag: str,
    trigger_hits: dict[str, list[str]],
) -> int:
    score = 0

    if event_type in {"Court Ruling", "Supreme Court Ruling", "Arrest / Detention", "Executive Order / Agency Action", "Military / Security Deployment"}:
        score += 3
    elif event_type == "Court Filing":
        score += 2
    elif event_type == "Developing / Unconfirmed":
        score += 1

    if source_role == "evidence":
        score += 2
    elif source_role in {"watchdog", "investigative"}:
        score += 1

    if source_tier == "Tier 1":
        score += 1

    if confidence == "High":
        score += 2
    elif confidence == "Medium":
        score += 1

    if oversight_failure_flag == "Yes":
        score += 3

    if len(trigger_hits) >= 3:
        score += 3
    elif len(trigger_hits) == 2:
        score += 2
    elif len(trigger_hits) == 1:
        score += 1

    if "weaponized_justice" in trigger_hits:
        score += 2
    if "court_defiance" in trigger_hits:
        score += 2
    if "election_interference" in trigger_hits:
        score += 2

    return score


def determine_cluster_status(cluster_score: int, threat_cluster: str) -> str:
    if not threat_cluster:
        return ""
    if cluster_score >= 10:
        return "Redline Watch"
    if cluster_score >= 7:
        return "Very Serious"
    if cluster_score >= 4:
        return "Serious"
    return "Emerging"


def compute_row_escalation_score(
    source_name: str,
    source_role: str,
    source_tier: str,
    confidence: str,
    src_priority: str,
    primary_signal: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
    event_definiteness: str,
    published_at: str,
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

    if src_priority in {"Top", "High"}:
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

    score += freshness_bonus(published_at)

    if is_repeat_prone_watchdog(source_name, source_role):
        score -= 2

    return max(score, 0)


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
    strong_count = strong_trigger_count(trigger_hits)

    if score_impact_candidate == "Unlikely":
        return "Medium" if strong_count >= 2 else "Low"

    if escalation_score >= 12 and strong_count >= 2:
        return "Urgent"
    if escalation_score >= 8 and (strong_count >= 1 or score_impact_candidate == "Likely"):
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


def suppress_repeaty_rows(
    rows: list[dict[str, str]],
    existing_title_stems: set[str],
) -> list[dict[str, str]]:
    seen_stems: set[str] = set(existing_title_stems)
    out: list[dict[str, str]] = []

    for row in rows:
        stem = title_stem(row.get("title", ""))
        source_name = row.get("source_name", "")
        source_role = row.get("source_role", "")

        if stem and is_repeat_prone_watchdog(source_name, source_role) and stem in seen_stems:
            continue

        if stem:
            seen_stems.add(stem)
        out.append(row)

    return out


def auto_notes(
    src_priority: str,
    admission: str,
    event_type: str,
    category_fit: str,
    democratic_consequence: str,
    trigger_hits: dict[str, list[str]],
    entity_hits: dict[str, list[str]],
    escalation_score: int,
    threat_cluster: str,
    cluster_status: str,
) -> str:
    parts = [
        f"source={src_priority}",
        f"admission={admission}",
        f"event={event_type}",
        f"fit={category_fit}",
        f"consequence={democratic_consequence}",
        f"score={escalation_score}",
    ]
    if threat_cluster:
        parts.append(f"cluster={threat_cluster}")
    if cluster_status:
        parts.append(f"cluster_status={cluster_status}")
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
    category_fit = classify_category_fit(category, primary_signal, trigger_hits, entity_hits)
    democratic_consequence = classify_democratic_consequence(
        event_type, category_fit, trigger_hits, primary_signal
    )

    admission = admission_decision(
        source_name=source_name,
        source_role=source_role,
        category_fit=category_fit,
        event_type=event_type,
        event_definiteness=event_definiteness,
        democratic_consequence=democratic_consequence,
        trigger_hits=trigger_hits,
        entity_hits=entity_hits,
        primary_signal=primary_signal,
        published_at=published_at,
    )

    oversight_failure_flag = determine_oversight_failure_flag(trigger_hits)
    governing_function = determine_governing_function(category, trigger_hits, oversight_failure_flag)
    threat_cluster = determine_threat_cluster(text, primary_signal, category, trigger_hits, oversight_failure_flag)
    cluster_score = compute_cluster_escalation_score(
        source_role=source_role,
        source_tier=source_tier,
        confidence=confidence,
        event_type=event_type,
        oversight_failure_flag=oversight_failure_flag,
        trigger_hits=trigger_hits,
    )
    cluster_status = determine_cluster_status(cluster_score, threat_cluster)

    row_score = compute_row_escalation_score(
        source_name=source_name,
        source_role=source_role,
        source_tier=source_tier,
        confidence=confidence,
        src_priority=src_priority,
        primary_signal=primary_signal,
        trigger_hits=trigger_hits,
        entity_hits=entity_hits,
        event_definiteness=event_definiteness,
        published_at=published_at,
    )

    score_impact_candidate = suggest_score_impact_candidate(
        escalation_score=row_score,
        admission=admission,
        primary_signal=primary_signal,
    )
    editor_priority = suggest_editor_priority(
        escalation_score=row_score,
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
        "threat_cluster": threat_cluster,
        "cluster_status": cluster_status,
        "cluster_escalation_score": str(cluster_score) if threat_cluster else "",
        "governing_function": governing_function,
        "oversight_failure_flag": oversight_failure_flag,
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
            escalation_score=row_score,
            threat_cluster=threat_cluster,
            cluster_status=cluster_status,
        ),
    }
    row["duplicate_cluster"] = make_duplicate_cluster_seed(row)
    return row


def refine_duplicate_clusters(rows: list[dict[str, str]]) -> None:
    seeds = [r.get("duplicate_cluster", "") for r in rows if r.get("duplicate_cluster", "")]
    counts = Counter(seeds)
    for row in rows:
        seed = row.get("duplicate_cluster", "")
        if seed and counts.get(seed, 0) < 2:
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


def parse_report_start_date() -> datetime:
    date_str = clean_text(RUNTIME_SETTINGS.get("last_published_report_date", ""))
    dt = iso_to_dt(date_str)
    if dt is not None:
        return dt

    default_days = int(RUNTIME_SETTINGS.get("default_lookback_days", 30))
    return datetime.now(timezone.utc) - timedelta(days=default_days)


def validate_rows(rows: list[dict[str, str]]) -> None:
    for i, row in enumerate(rows, start=1):
        missing = [h for h in HEADERS if h not in row]
        extra = [k for k in row if k not in HEADERS]
        if missing or extra:
            raise ValueError(f"Row {i} schema mismatch. Missing={missing} Extra={extra}")


def write_csv(rows: list[dict[str, str]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    max_items = int(os.getenv("MAX_ITEMS_PER_RUN", "100"))
    report_start_dt = parse_report_start_date()

    print(f"Using report window start: {dt_to_iso(report_start_dt)}")

    existing_links, existing_titles = get_existing_sheet_values(
        worksheet_name="Intake",
        headers=HEADERS,
        max_title_rows=500,
    )
    existing_title_stems = {title_stem(t) for t in existing_titles if title_stem(t)}

    print(f"Loaded {len(existing_links)} existing links from Intake.")
    print(f"Loaded {len(existing_title_stems)} existing title stems from Intake.")

    items = fetch_all_feeds()
    rows: list[dict[str, str]] = []

    for item in items:
        published_dt = iso_to_dt(getattr(item, "published_at", None))
        if published_dt and published_dt < report_start_dt:
            continue

        row = build_row(item)
        if row["admission_decision"] == "Reject":
            continue

        link = row.get("link", "").strip()
        if link and link in existing_links:
            continue

        if (
            is_repeat_prone_watchdog(row.get("source_name", ""), row.get("source_role", ""))
            and title_stem(row.get("title", "")) in existing_title_stems
        ):
            continue

        rows.append(row)

    rows = dedupe_rows_by_link(rows)
    rows = suppress_repeaty_rows(rows, existing_title_stems)
    refine_duplicate_clusters(rows)
    rows = rows[:max_items]

    validate_rows(rows)
    write_csv(rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT_CSV}")

    print(f"Rows after classification/dedupe: {len(rows)}")
    print("Generated links for this run:")
    for row in rows[:20]:
        print(f"{row.get('published_at', '')} | {row.get('source_name', '')} | {row.get('title', '')}")
        print(f"LINK: {row.get('link', '')}")

    try:
        new_count = append_rows_to_sheet(rows, worksheet_name="Intake", headers=HEADERS)
        if new_count == 0:
            print("No new rows to append after dedupe.")
        print(f"Appended {new_count} new rows to worksheet 'Intake'.")
    except Exception as e:
        print(f"Google Sheets append failed: {e}")


if __name__ == "__main__":
    main()
