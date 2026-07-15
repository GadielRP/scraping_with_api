"""Bulk attachment of persisted Oddspapi fixture IDs to pre-start candidates."""

from __future__ import annotations

from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)

from .constants import ODDSPAPI_SOURCE
from .event_selector import OddspapiPreStartCandidate


def attach_oddspapi_fixture_ids(
    candidates: list[OddspapiPreStartCandidate],
    session: Session,
) -> list[OddspapiPreStartCandidate]:
    """Attach mappings in one query, retaining unmapped candidates for metrics."""
    mappings = EventSourceMappingRepository.get_source_event_ids_by_event_ids(
        event_ids=[candidate.event_id for candidate in candidates],
        source=ODDSPAPI_SOURCE,
        session=session,
    )
    for candidate in candidates:
        candidate.fixture_id = mappings.get(candidate.event_id)
    return candidates
