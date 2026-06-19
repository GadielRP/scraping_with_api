"""Helpers for resolving external SofaScore event IDs from canonical IDs."""

from __future__ import annotations

from infrastructure.persistence.repositories import EventSourceMappingRepository


def resolve_sofascore_event_id(event_id: int) -> int:
    """Resolve a canonical event ID to the external SofaScore event ID.

    Raises:
        ValueError: If the mapping is missing or the resolved source ID is not numeric.
    """

    source_event_id = EventSourceMappingRepository.resolve_required_source_event_id(
        event_id=event_id,
        source="sofascore",
    )

    try:
        return int(source_event_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Missing numeric SofaScore source_event_id for event_id={event_id}: {source_event_id}"
        ) from exc
