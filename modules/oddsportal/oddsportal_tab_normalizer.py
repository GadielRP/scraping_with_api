"""Tab label normalization helpers for OddsPortal/CuotasAhora."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List, Optional

GROUP_LABELS = {
    "en": {
        "1X2": ["1X2"],
        "HOME_AWAY": ["Home/Away", "Home / Away"],
        "OVER_UNDER": ["Over/Under", "Over / Under"],
        "ASIAN_HANDICAP": ["Asian Handicap"],
    },
    "es": {
        "1X2": ["1X2"],
        "HOME_AWAY": ["Local/Visitante", "Local / Visitante"],
        "OVER_UNDER": ["Más/Menos de", "Mas/Menos de", "Más / Menos de", "Mas / Menos de"],
        "ASIAN_HANDICAP": ["Hándicap asiático", "Handicap asiatico", "Handicap asiático", "Hándicap asiatico"],
    },
}

PERIOD_LABELS = {
    "en": {
        "FT_INC_OT": [
            "Full Time",
            "FT including OT",
            "Full Time including Overtime",
        ],
        "FULL_TIME": [
            "Full Time",
            "Full time",
            "FT",
        ],
        "1ST_HALF": ["1st Half", "1st half"],
        "2ND_HALF": ["2nd Half", "2nd half"],
        "1ST_QUARTER": ["1st Quarter", "1st quarter"],
        "2ND_QUARTER": ["2nd Quarter", "2nd quarter"],
        "3RD_QUARTER": ["3rd Quarter", "3rd quarter"],
        "4TH_QUARTER": ["4th Quarter", "4th quarter"],
        "1ST_PERIOD": ["1st Period", "1st period"],
        "2ND_PERIOD": ["2nd Period", "2nd period"],
        "3RD_PERIOD": ["3rd Period", "3rd period"],
    },
    "es": {
        "FT_INC_OT": [
            "Final del partido incluyendo prórroga",
            "Final del partido incluyendo proroga",
        ],
        "FULL_TIME": [
            "Final del partido",
        ],
        "1ST_HALF": ["1er tiempo"],
        "2ND_HALF": ["2° tiempo", "2º tiempo"],
        "1ST_QUARTER": ["1er cuarto"],
        "2ND_QUARTER": ["2° cuarto", "2º cuarto"],
        "3RD_QUARTER": ["3er cuarto"],
        "4TH_QUARTER": ["4° cuarto", "4º cuarto"],
        "1ST_PERIOD": ["1er periodo", "1er período"],
        "2ND_PERIOD": ["2° periodo", "2º periodo", "2° período", "2º período"],
        "3RD_PERIOD": ["3er periodo", "3er período"],
    },
}


def normalize_tab_label(value: Optional[str]) -> str:
    if value is None:
        return ""

    normalized = re.sub(r"\s+", " ", value.strip().lower())
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", normalized).strip()


def _resolve_languages(language: Optional[str]) -> List[str]:
    lang = (language or "en").strip().lower()
    if lang == "auto":
        return ["en", "es"]
    if lang == "es":
        return ["es"]
    return ["en"]


def _dedupe_candidates(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        label = (value or "").strip()
        if not label:
            continue
        normalized = normalize_tab_label(label)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(label)
    return out


def _build_candidates(
    labels_map: dict,
    market_key: Optional[str],
    display_name: Optional[str],
    language: Optional[str],
) -> List[str]:
    resolved_languages = _resolve_languages(language)
    raw_candidates: List[str] = []
    key = (market_key or "").strip()

    for resolved_language in resolved_languages:
        language_labels = labels_map.get(resolved_language, {})
        raw_candidates.extend(language_labels.get(key, []))

    if display_name:
        raw_candidates.append(display_name)

    return _dedupe_candidates(raw_candidates)


def get_group_tab_candidates(
    group_key: Optional[str],
    display_name: Optional[str],
    language: Optional[str],
) -> List[str]:
    return _build_candidates(GROUP_LABELS, group_key, display_name, language)


def get_period_tab_candidates(
    period_key: Optional[str],
    display_name: Optional[str],
    language: Optional[str],
) -> List[str]:
    return _build_candidates(PERIOD_LABELS, period_key, display_name, language)


def tab_label_matches(actual_label: str, candidates: Iterable[str]) -> bool:
    actual_norm = normalize_tab_label(actual_label)
    if not actual_norm:
        return False
    return any(actual_norm == normalize_tab_label(candidate) for candidate in candidates)
