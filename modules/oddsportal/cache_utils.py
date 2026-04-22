import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .dataclasses import CacheQualityMetrics
from .oddsportal_config import get_oddsportal_current_date


DEBUG_TIMING = os.getenv("DEBUG_TIMING", "false").lower() == "true"
ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_GOTO_TIMEOUT_MS", "21000"))
ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS = int(os.getenv("ODDSPORTAL_LEAGUE_ROWS_TIMEOUT_MS", "18000"))
ODDSPORTAL_SESSION_RESTART_ATTEMPTS = int(os.getenv("ODDSPORTAL_SESSION_RESTART_ATTEMPTS", "2"))
EN_DASH = "\u2013"
TEAM_SEPARATOR_PATTERN = rf"\s+(?:vs|[{EN_DASH}-])\s+"
LEGACY_CACHE_MATCH_PATTERN = rf"([^\n{EN_DASH}\-]+)[\s\n]+(?:vs|[{EN_DASH}v\-])[\s\n]+([^\n\d]+)"
TEAM_PREFIX_CLEAN_PATTERN = rf"(^.*?\d{{2}}:\d{{2}}\s+|^\w+,\s+\d{{1,2}}\s+\w+\s+[{EN_DASH}-]\s+|^.*?\d{{1,2}}:\d{{2}}\s+)"
ODDSPORTAL_CACHE_DATE_FORMATS = (
    "%d %b %Y",
    "%d %B %Y",
    "%a %d %b %Y",
    "%A %d %B %Y",
    "%d %b",
    "%d %B",
)
ODDSPORTAL_RELATIVE_DATE_OFFSETS = {
    "today": 0,
    "tomorrow": 1,
    "yesterday": -1,
}


def log_timing(msg):
    if DEBUG_TIMING:
        print(f"⏱️ [Timing] {msg}")


def _normalize_league_url(league_url: Optional[str]) -> Optional[str]:
    """Normalize league URLs so grouping and cache keys stay stable."""
    if not league_url:
        return None
    normalized = league_url.strip()
    if not normalized:
        return None
    return normalized.rstrip("/")


def _build_league_group_key(season_id: Optional[int], league_url: Optional[str]) -> Optional[Tuple[int, str]]:
    normalized_league_url = _normalize_league_url(league_url)
    if not season_id or not normalized_league_url:
        return None
    return (season_id, normalized_league_url)


def _coerce_current_date(current_date: Optional[date] = None) -> date:
    if isinstance(current_date, datetime):
        return current_date.date()
    if isinstance(current_date, date):
        return current_date
    return get_oddsportal_current_date()


def _parse_oddsportal_cache_date(date_text: Any, current_date: Optional[date] = None) -> Optional[date]:
    reference_date = _coerce_current_date(current_date)
    if date_text is None:
        return None

    normalized_text = re.sub(r"\s+", " ", str(date_text).replace(",", " ")).strip()
    if not normalized_text:
        return None

    iso_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", normalized_text)
    if iso_match:
        try:
            return date.fromisoformat(iso_match.group(0))
        except ValueError:
            pass

    explicit_date_match = re.search(r"\b\d{1,2}\s+[A-Za-z]{3,9}(?:\s+\d{4})?\b", normalized_text)
    if explicit_date_match:
        candidate_text = explicit_date_match.group(0)
        for date_format in ODDSPORTAL_CACHE_DATE_FORMATS:
            try:
                parsed = datetime.strptime(candidate_text, date_format).date()
            except ValueError:
                continue

            if "%Y" in date_format:
                return parsed

            parsed = parsed.replace(year=reference_date.year)
            if parsed < reference_date and reference_date.month == 12 and parsed.month == 1:
                parsed = parsed.replace(year=reference_date.year + 1)
            return parsed

    lower_text = normalized_text.lower()
    for relative_token, offset in ODDSPORTAL_RELATIVE_DATE_OFFSETS.items():
        if lower_text == relative_token or lower_text.startswith(f"{relative_token} "):
            return reference_date + timedelta(days=offset)

    return None


def _normalize_cache_date(date_text: str, reference_date: Optional[date] = None) -> str:
    """Resolve relative dates to absolute 'DD Mon YYYY' format."""
    if not date_text:
        return date_text
    parsed = _parse_oddsportal_cache_date(date_text, reference_date)
    if parsed is None:
        return date_text
    return parsed.strftime("%d %b %Y")


def _build_structured_league_cache(
    candidates: List[Dict[str, str]],
    current_date: Optional[date] = None,
) -> Dict[str, Dict[str, str]]:
    reference_date = _coerce_current_date(current_date)
    return {
        candidate["href"]: {
            "home": candidate["home"],
            "away": candidate["away"],
            "raw_text": candidate.get("raw_text", ""),
            "date": _normalize_cache_date(candidate.get("date", ""), reference_date),
        }
        for candidate in candidates
        if candidate.get("href")
    }


def _is_cache_date_current_or_future(date_text: Any, current_date: Optional[date] = None) -> bool:
    reference_date = _coerce_current_date(current_date)
    parsed_date = _parse_oddsportal_cache_date(date_text, reference_date)
    return parsed_date is not None and parsed_date >= reference_date


def _calculate_cache_homogeneity(endpoints: List[str]) -> float:
    if not endpoints:
        return 0.0

    prefix_counts = {}

    for ep in endpoints:
        parts = [p for p in ep.split("/") if p]
        if len(parts) >= 2:
            prefix = f"/{parts[0]}/{parts[1]}/"
            prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
        else:
            prefix_counts["unknown"] = prefix_counts.get("unknown", 0) + 1

    max_prefix_count = max(prefix_counts.values()) if prefix_counts else 0
    return max_prefix_count / len(endpoints)


def _evaluate_cache_quality(
    cache_dict: Dict[str, Any],
    current_date: Optional[date] = None,
) -> CacheQualityMetrics:
    """Evaluate cache quality while strongly preferring fresh-dated entries."""
    if not cache_dict:
        return CacheQualityMetrics(
            total_count=0,
            fresh_count=0,
            stale_count=0,
            freshness_ratio=0.0,
            homogeneity=0.0,
            score=0.0,
        )

    reference_date = _coerce_current_date(current_date)
    endpoints = [href for href in cache_dict.keys() if href]
    fresh_endpoints = []

    for href, cache_entry in cache_dict.items():
        if not href:
            continue
        entry_date_text = cache_entry.get("date", "") if isinstance(cache_entry, dict) else ""
        if _is_cache_date_current_or_future(entry_date_text, reference_date):
            fresh_endpoints.append(href)

    total_count = len(endpoints)
    fresh_count = len(fresh_endpoints)
    stale_count = max(0, total_count - fresh_count)
    freshness_ratio = (fresh_count / total_count) if total_count else 0.0
    homogeneity_basis = fresh_endpoints if fresh_endpoints else endpoints
    homogeneity = _calculate_cache_homogeneity(homogeneity_basis)
    score = fresh_count * homogeneity

    return CacheQualityMetrics(
        total_count=total_count,
        fresh_count=fresh_count,
        stale_count=stale_count,
        freshness_ratio=freshness_ratio,
        homogeneity=homogeneity,
        score=score,
    )


def _format_group_key(group_key: Optional[Tuple[int, str]]) -> str:
    if not group_key:
        return "(non-primable)"
    season_id, league_url = group_key
    return f"(season_id={season_id}, league_url={league_url})"

