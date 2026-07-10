"""OddsPapi period alias resolution for catalog market matching."""

from __future__ import annotations

from modules.oddspapi.format_utils import normalized_compact

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
EXTRA_TIME_PERIOD_ALIASES = {"extratime", "overtime"}
SECOND_HALF_PERIOD_ALIASES = {"2ndhalf", "secondhalf"}


def resolve_canonical_period(item: dict) -> tuple[str | None, str | None]:
    """Return (canonical_period_label, period_suffix_or_reason)."""
    period_compact = normalized_compact(item.get("period"))
    market_name_compact = normalized_compact(item.get("marketName"))

    if period_compact in FULL_TIME_PERIOD_ALIASES:
        return "Full Time", "full_time"

    if period_compact in FIRST_QUARTER_PERIOD_ALIASES:
        return "1st Quarter", "1st_quarter"

    if period_compact in FIRST_HALF_PERIOD_ALIASES:
        return "1st Half", "1st_half"

    if period_compact in FIRST_SET_PERIOD_ALIASES:
        return "1st Set", "1st_set"

    if period_compact in CURRENT_SET_PERIOD_ALIASES:
        return "Current Set", "current_set"

    if period_compact in FIRST_PERIOD_PERIOD_ALIASES:
        return "1st Period", "1st_period"

    if period_compact in EXTRA_TIME_PERIOD_ALIASES:
        return "Extra Time", "extra_time"

    if period_compact == "p1":
        if "1sthalf" in market_name_compact or "firsthalf" in market_name_compact:
            return "1st Half", "1st_half"
        return None, "unsupported_period_context"

    if period_compact in SECOND_HALF_PERIOD_ALIASES:
        return None, "unsupported_period"

    if period_compact == "p2":
        if "2ndhalf" in market_name_compact or "secondhalf" in market_name_compact:
            return None, "unsupported_period"
        return None, "unsupported_period_context"

    return None, "unsupported_period"
