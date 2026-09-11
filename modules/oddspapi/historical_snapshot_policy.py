"""Oddspapi policy for canonical historical snapshot persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any


def provider_instant(value: Any) -> datetime | None:
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


def _moment_value(value: Any) -> Decimal | None:
    """Parse a logical minutes-until-start value without losing precision."""
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def should_persist_current_snapshot(
    player: dict,
    *,
    deduplicate: bool,
    current_moment_minutes: int | float | None = None,
) -> bool:
    """Return False only for the same tick at the same extraction checkpoint.

    ``createdAt`` identifies the provider tick, while ``minutesUntilStart``
    identifies the historical checkpoint represented by a moment quote.  A
    current snapshot from T-0 must therefore not be deduplicated merely because
    it shares a carried T-5 tick with a T-5 moment quote.
    """
    if not deduplicate:
        return True

    extraction_minute = _moment_value(current_moment_minutes)
    if extraction_minute is None:
        # Without the logical extraction checkpoint, retaining current is the
        # only safe behavior.  The caller may be a direct historical reader or
        # a backfill that has no candidate timing context.
        return True

    current_instant = provider_instant(player.get("changedAt"))
    if current_instant is None:
        return True

    moment_quotes = player.get("momentQuotes")
    if not isinstance(moment_quotes, list):
        return True

    return not any(
        isinstance(moment, dict)
        and _moment_value(moment.get("minutesUntilStart")) == extraction_minute
        and provider_instant(moment.get("createdAt")) == current_instant
        for moment in moment_quotes
    )


_CURRENT_OBSERVATION_FIELDS = (
    "price",
    "active",
    "limit",
    "changedAt",
    "sourceCollectedAt",
    "exchangeMeta",
)


def select_latest_current_player(
    base_player: dict,
    historical_player: dict,
) -> dict:
    """Select the newest current observation for one matching player.

    The base player normally comes from ``/odds`` and the historical player
    from ``/historical-odds``.  Only fields that describe the current
    observation are replaced; market identity, main-line metadata and opening
    fields remain owned by the base payload. Equal timestamps prefer the base
    payload because it is the richer live representation; if either side has
    no usable provider timestamp, the base payload is retained safely.
    """
    base_instant = provider_instant(
        base_player.get("changedAt") or base_player.get("sourceCollectedAt")
    )
    historical_instant = provider_instant(
        historical_player.get("changedAt")
        or historical_player.get("sourceCollectedAt")
    )

    if base_instant is None or historical_instant is None:
        # The /odds endpoint is the live base. Without a timestamp on it we
        # cannot prove that historical is newer, so retain the base safely.
        return base_player
    if historical_instant <= base_instant:
        return base_player

    selected = dict(base_player)
    for field_name in _CURRENT_OBSERVATION_FIELDS:
        if field_name in historical_player:
            selected[field_name] = historical_player[field_name]
        elif field_name == "sourceCollectedAt":
            # Do not let a stale /odds override hide the newer historical
            # changedAt value from the canonical adapter payload.
            selected.pop(field_name, None)
    return selected
