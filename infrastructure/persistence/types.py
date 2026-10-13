"""Reusable SQLAlchemy types that enforce persistence contracts."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Numeric, Text
from decimal import Decimal
from sqlalchemy.types import TypeDecorator

from shared.temporal import UTC, as_utc


class UTCDateTime(TypeDecorator):
    """Persist and return absolute instants as aware UTC datetimes.

    PostgreSQL uses ``TIMESTAMP WITH TIME ZONE``. SQLite has no equivalent, so
    tests store UTC-naive values physically and restore ``tzinfo=UTC`` when
    loading. The public Python contract remains timezone-aware on both dialects.
    """

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(DateTime(timezone=dialect.name == "postgresql"))

    def process_bind_param(self, value: datetime | None, dialect):
        if value is None:
            return None
        normalized = as_utc(value, field_name="UTCDateTime value")
        if dialect.name == "sqlite":
            return normalized.replace(tzinfo=None)
        return normalized

    def process_result_value(self, value: datetime | None, dialect):
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            if dialect.name == "sqlite":
                return value.replace(tzinfo=UTC)
            raise ValueError(
                "PostgreSQL returned a naive datetime for UTCDateTime; "
                "the deployed column has not been migrated to timestamptz"
            )
        return as_utc(value, field_name="UTCDateTime database value")


class ExactNumeric(TypeDecorator):
    """Unconstrained PostgreSQL numeric, lossless decimal text in SQLite tests."""

    impl = Numeric
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return dialect.type_descriptor(
            Text() if dialect.name == "sqlite" else Numeric()
        )

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        decimal = Decimal(str(value))
        if not decimal.is_finite():
            raise ValueError("numeric values must be finite")
        return str(decimal) if dialect.name == "sqlite" else decimal

    def process_result_value(self, value, dialect):
        return None if value is None else Decimal(str(value))


__all__ = ["UTCDateTime", "ExactNumeric"]
