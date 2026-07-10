"""OddsPapi period alias resolution for catalog market matching."""

from __future__ import annotations

from modules.oddspapi.format_utils import normalize_source_id, normalized_compact

FULL_TIME_PERIOD_ALIASES = {
    "fulltime",
    "ft",
    "match",
    "result",
    "full-time",
    "fulltime(includingovertime)",
}
FIRST_QUARTER_PERIOD_ALIASES = {"1stquarter", "firstquarter"}
FIRST_HALF_PERIOD_ALIASES = {"1sthalf", "firsthalf"}
FIRST_SET_PERIOD_ALIASES = {"1stset", "firstset"}
CURRENT_SET_PERIOD_ALIASES = {"currentset"}
FIRST_PERIOD_PERIOD_ALIASES = {"1stperiod", "firstperiod"}
EXTRA_TIME_PERIOD_ALIASES = {"extratime", "overtime", "result"}
SECOND_HALF_PERIOD_ALIASES = {"2ndhalf", "secondhalf"}


def resolve_canonical_period(item: dict) -> tuple[str | None, str | None]:
    """Return (canonical_period_label, period_suffix_or_reason)."""
    period_compact = normalized_compact(item.get("period"))
    market_name_compact = normalized_compact(item.get("marketName"))
    sport_id = normalize_source_id(item.get("sportId"))

    if not period_compact:
        if "firstquarter" in market_name_compact or "1stquarter" in market_name_compact:
            return "1st Quarter", "1st_quarter"
        if "firsthalf" in market_name_compact or "1sthalf" in market_name_compact:
            return "1st Half", "1st_half"
        if "firstset" in market_name_compact or "1stset" in market_name_compact:
            return "1st Set", "1st_set"
        if "firstperiod" in market_name_compact or "1stperiod" in market_name_compact:
            return "1st Period", "1st_period"
        if "firstinning" in market_name_compact or "1stinning" in market_name_compact:
            return "1st Inning", "1st_inning"
        if "fulltime" in market_name_compact or "full-time" in market_name_compact or "match" in market_name_compact:
            return "Full Time", "full_time"

    if period_compact in FULL_TIME_PERIOD_ALIASES:
        return "Full Time", "full_time"

    # Sport-aware resolution for generic period codes (p1, p2, p3, p4, p5, etc.)
    if period_compact == "p1":
        if sport_id in {"11", "14"}:  # Basketball, American Football
            return "1st Quarter", "1st_quarter"
        elif sport_id in {"12", "23"}:  # Tennis, Volleyball
            return "1st Set", "1st_set"
        elif sport_id == "15":  # Ice Hockey
            return "1st Period", "1st_period"
        elif sport_id == "13":  # Baseball
            return "1st Inning", "1st_inning"
        
        # Fallbacks based on market names
        if "1sthalf" in market_name_compact or "firsthalf" in market_name_compact:
            return "1st Half", "1st_half"
        return None, "unsupported_period_context"

    if period_compact == "p2":
        if sport_id in {"11", "14"}:
            return "2nd Quarter", "2nd_quarter"
        elif sport_id in {"12", "23"}:
            return "2nd Set", "2nd_set"
        elif sport_id == "15":
            return "2nd Period", "2nd_period"
        elif sport_id == "13":
            return "2nd Inning", "2nd_inning"
        
        if "2ndhalf" in market_name_compact or "secondhalf" in market_name_compact:
            return None, "unsupported_period"
        return None, "unsupported_period_context"

    if period_compact == "p3":
        if sport_id in {"11", "14"}:
            return "3rd Quarter", "3rd_quarter"
        elif sport_id in {"12", "23"}:
            return "3rd Set", "3rd_set"
        elif sport_id == "15":
            return "3rd Period", "3rd_period"
        elif sport_id == "13":
            return "3rd Inning", "3rd_inning"
        return None, "unsupported_period_context"

    if period_compact == "p4":
        if sport_id in {"11", "14"}:
            return "4th Quarter", "4th_quarter"
        elif sport_id in {"12", "23"}:
            return "4th Set", "4th_set"
        elif sport_id == "13":
            return "4th Inning", "4th_inning"
        return None, "unsupported_period_context"

    if period_compact == "p5":
        if sport_id in {"12", "23"}:
            return "5th Set", "5th_set"
        elif sport_id == "13":
            return "5th Inning", "5th_inning"
        return None, "unsupported_period_context"

    # Multi-period codes
    if period_compact == "p1+p2":
        if sport_id in {"10", "11", "14", "22", "26"}:  # Soccer, Basketball, NFL, Handball, Rugby
            return "1st Half", "1st_half"
        return None, "unsupported_period_context"

    if period_compact in {"p3+p4", "p3+p4+overtime"}:
        if sport_id in {"11", "14"}:
            return "2nd Half", "2nd_half"
        return None, "unsupported_period_context"

    # Static/explicit period name matching
    if period_compact in FIRST_QUARTER_PERIOD_ALIASES:
        return "1st Quarter", "1st_quarter"

    if period_compact in FIRST_HALF_PERIOD_ALIASES:
        return "1st Half", "1st_half"

    if period_compact in FIRST_SET_PERIOD_ALIASES:
        return "1st Set", "1st_set"

    if period_compact in FIRST_PERIOD_PERIOD_ALIASES:
        return "1st Period", "1st_period"

    if period_compact in CURRENT_SET_PERIOD_ALIASES:
        return "Current Set", "current_set"

    if period_compact in EXTRA_TIME_PERIOD_ALIASES:
        return "Extra Time", "extra_time"

    if period_compact in SECOND_HALF_PERIOD_ALIASES:
        return None, "unsupported_period"

    return None, "unsupported_period"
