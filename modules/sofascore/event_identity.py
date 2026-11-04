"""Resolve canonical and SofaScore identities for an event."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from infrastructure.persistence.repositories import EventSourceMappingRepository

EventIdType = Literal["canonical", "sofascore"]


@dataclass(frozen=True)
class SofaScoreEventIdentity:
    """Known identifiers for one event; canonical ID can be unknown."""

    canonical_event_id: int | None
    sofascore_event_id: int


def resolve_sofascore_event_identity(
    event_id: int,
    *,
    input_id_type: EventIdType = "canonical",
) -> SofaScoreEventIdentity:
    """Resolve an event ID into its canonical and SofaScore identifiers.

    With ``input_id_type="canonical"``, the SofaScore mapping is required.
    With ``input_id_type="sofascore"``, the canonical mapping is optional and
    remains ``None`` when this provider ID has not been mapped yet.
    """
    try:
        numeric_event_id = int(event_id)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Event ID must be numeric: {event_id!r}") from exc

    if input_id_type == "canonical":
        source_event_id = EventSourceMappingRepository.resolve_required_source_event_id(
            event_id=numeric_event_id,
            source="sofascore",
        )
        try:
            sofascore_event_id = int(source_event_id)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Missing numeric SofaScore source_event_id for "
                f"event_id={numeric_event_id}: {source_event_id}"
            ) from exc
        return SofaScoreEventIdentity(numeric_event_id, sofascore_event_id)

    if input_id_type == "sofascore":
        canonical_event_id = EventSourceMappingRepository.get_event_id_by_source(
            "sofascore",
            str(numeric_event_id),
        )
        return SofaScoreEventIdentity(canonical_event_id, numeric_event_id)

    raise ValueError(f"Unsupported input_id_type: {input_id_type!r}")
