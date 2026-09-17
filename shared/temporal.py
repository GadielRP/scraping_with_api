"""Canonical temporal primitives for absolute instants.

Domain and persistence code must exchange timezone-aware ``datetime`` values.
UTC is the canonical representation for calculations and provider boundaries;
named local zones are reserved for calendar rules and presentation.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo


UTC = timezone.utc


class NaiveDateTimeError(ValueError):
    """Raised when an absolute instant is supplied without timezone context."""


def require_aware(value: datetime, *, field_name: str = "datetime") -> datetime:
    """Return ``value`` after proving that it identifies an absolute instant."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None or value.utcoffset() is None:
        raise NaiveDateTimeError(
            f"{field_name} must be timezone-aware; naive datetimes are not valid instants"
        )
    return value


def as_utc(value: datetime, *, field_name: str = "datetime") -> datetime:
    """Normalize one aware instant to UTC without changing the instant."""
    return require_aware(value, field_name=field_name).astimezone(UTC)


def utc_now() -> datetime:
    """Return the current absolute instant as an aware UTC datetime."""
    return datetime.now(UTC)


def from_unix_timestamp(value: int | float) -> datetime:
    """Parse a Unix timestamp as an aware UTC instant."""
    return datetime.fromtimestamp(value, tz=UTC)


def in_timezone(value: datetime, timezone_name: str) -> datetime:
    """Render an aware instant in a named IANA timezone."""
    return as_utc(value).astimezone(ZoneInfo(timezone_name))


def interpret_local_naive(
    value: datetime,
    timezone_name: str,
    *,
    field_name: str = "datetime",
) -> datetime:
    """Interpret a legacy naive wall-clock value and return its UTC instant.

    This function exists only for explicit legacy/integration boundaries. New
    domain values must already be aware and should use :func:`as_utc` instead.
    """
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is not None and value.utcoffset() is not None:
        raise ValueError(f"{field_name} is already timezone-aware")
    return value.replace(tzinfo=ZoneInfo(timezone_name)).astimezone(UTC)


def local_day_bounds_utc(
    local_date: date,
    timezone_name: str,
) -> tuple[datetime, datetime]:
    """Return the half-open UTC bounds for one civil day in ``timezone_name``."""
    if isinstance(local_date, datetime):
        local_date = local_date.date()
    if not isinstance(local_date, date):
        raise TypeError("local_date must be a date")

    zone = ZoneInfo(timezone_name)
    local_start = datetime.combine(local_date, time.min, tzinfo=zone)
    next_date = local_date.fromordinal(local_date.toordinal() + 1)
    local_end = datetime.combine(next_date, time.min, tzinfo=zone)
    return local_start.astimezone(UTC), local_end.astimezone(UTC)


__all__ = [
    "NaiveDateTimeError",
    "UTC",
    "as_utc",
    "from_unix_timestamp",
    "in_timezone",
    "interpret_local_naive",
    "local_day_bounds_utc",
    "require_aware",
    "utc_now",
]
