"""Compatibility exports for shared discovery event filters."""

from modules.jobs.discovery_filters import (
    filter_upcoming_events,
    is_supported_sport,
    is_supported_sport_name,
)

__all__ = ["filter_upcoming_events", "is_supported_sport", "is_supported_sport_name"]
