"""OddsPortal tooltip timestamp parsing and year inference."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta


_MONTHS = {
    "jan": 1,
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "ago": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
    "dic": 12,
}


def parse_oddsportal_tooltip_time(
    value: str | None,
    *,
    reference_time: datetime | None = None,
) -> datetime | None:
    """Parse ``05 Aug, 10:01`` and infer the nearest non-future year."""

    match = re.fullmatch(
        r"\s*(\d{1,2})\s+([^\s,<]{3,12}),\s*(\d{2}):(\d{2})\s*",
        value or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    day, raw_month, hour, minute = match.groups()
    normalized_month = unicodedata.normalize("NFKD", raw_month)
    normalized_month = "".join(
        char for char in normalized_month if not unicodedata.combining(char)
    ).lower().rstrip(".")
    month = _MONTHS.get(normalized_month)
    if month is None:
        return None

    reference = reference_time or datetime.now()
    try:
        candidate = datetime(
            reference.year,
            month,
            int(day),
            int(hour),
            int(minute),
        )
    except ValueError:
        return None

    # A December movement shown around New Year belongs to the previous year.
    # The tolerance avoids treating small page/host clock skew as a year change.
    if candidate > reference + timedelta(days=2):
        try:
            candidate = candidate.replace(year=reference.year - 1)
        except ValueError:
            return None
    return candidate


def oddsportal_tooltip_time_to_iso(
    value: str | None,
    *,
    reference_time: datetime | None = None,
) -> str | None:
    parsed = parse_oddsportal_tooltip_time(
        value,
        reference_time=reference_time,
    )
    return parsed.isoformat(timespec="minutes") if parsed else None


__all__ = [
    "oddsportal_tooltip_time_to_iso",
    "parse_oddsportal_tooltip_time",
]
