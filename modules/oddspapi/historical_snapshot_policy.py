"""Oddspapi policy for canonical historical snapshot persistence."""

from __future__ import annotations

from datetime import datetime, timezone
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
        historical_player.get("changedAt") or historical_player.get("sourceCollectedAt")
    )

    if base_instant is None or historical_instant is None:
        # The /odds endpoint is the live base. Without a timestamp on it we
        # cannot prove that historical is newer, so retain the base safely.
        return base_player
    if historical_instant <= base_instant:
        return base_player

    selected = dict(base_player)
    for field_name in _CURRENT_OBSERVATION_FIELDS:
        if field_name == "sourceCollectedAt":
            if historical_player.get(field_name) not in (None, ""):
                selected[field_name] = historical_player[field_name]
            else:
                # Do not let a stale /odds value hide the newer historical
                # changedAt value from the canonical adapter payload. The
                # repository falls back to changedAt when this field is absent.
                selected.pop(field_name, None)
            continue

        if field_name not in historical_player:
            continue

        historical_value = historical_player[field_name]
        if historical_value in (None, ""):
            # /historical-odds is intentionally sparse. A null limit or
            # exchangeMeta means that the historical endpoint did not provide
            # that field, not that a richer /odds value should be erased.
            continue

        selected[field_name] = historical_value
    return selected
