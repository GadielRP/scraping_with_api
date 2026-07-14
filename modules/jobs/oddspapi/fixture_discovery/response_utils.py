"""Small defensive helpers for Oddspapi responses and UTC windows."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
from typing import Any

logger = logging.getLogger(__name__)


def extract_fixture_list(payload: dict | list) -> list[dict]:
    """Extract fixture dictionaries from raw or common wrapped responses."""
    candidates: Any = payload
    if isinstance(payload, dict):
        for key in ("fixtures", "data", "items"):
            if key in payload:
                candidates = payload[key]
                break
        else:
            logger.warning("Unsupported Oddspapi fixtures response keys=%s", sorted(payload))
            return []

    if not isinstance(candidates, list):
        logger.warning("Unsupported Oddspapi fixtures response shape=%s", type(candidates).__name__)
        return []

    fixtures = [item for item in candidates if isinstance(item, dict)]
    invalid_items = len(candidates) - len(fixtures)
    if invalid_items:
        logger.warning("Ignored %s non-object Oddspapi fixture payload(s)", invalid_items)
    return fixtures


def to_oddspapi_iso(dt: datetime) -> str:
    """Format a datetime as the UTC ISO string expected by Oddspapi."""
    if not isinstance(dt, datetime):
        raise TypeError("datetime is required")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc_dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def as_utc_datetime(value: str | datetime | date) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, datetime.min.time())
    else:
        text = str(value).strip()
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def split_time_window(
    from_dt: datetime,
    to_dt: datetime,
    max_window_hours: int = 48,
) -> list[tuple[datetime, datetime]]:
    """Split a UTC window into chunks no larger than ``max_window_hours``."""
    start = as_utc_datetime(from_dt)
    end = as_utc_datetime(to_dt)
    if end <= start:
        raise ValueError("to_dt must be after from_dt")
    if max_window_hours <= 0:
        raise ValueError("max_window_hours must be positive")

    maximum = timedelta(hours=max_window_hours)
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + maximum, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end
    return chunks
