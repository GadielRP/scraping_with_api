"""Oddspapi policy for canonical historical snapshot persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _provider_instant(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def should_persist_current_snapshot(
    player: dict,
    *,
    deduplicate: bool,
) -> bool:
    """Return False when ``current`` is already one of the historical ticks."""
    if not deduplicate:
        return True

    current_instant = _provider_instant(player.get("changedAt"))
    if current_instant is None:
        return True

    moment_quotes = player.get("momentQuotes")
    if not isinstance(moment_quotes, list):
        return True

    return not any(
        isinstance(moment, dict)
        and _provider_instant(moment.get("createdAt")) == current_instant
        for moment in moment_quotes
    )
