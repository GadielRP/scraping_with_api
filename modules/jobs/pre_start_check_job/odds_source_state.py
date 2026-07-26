"""Bulk provider-state loading for the pre-start odds flows."""

from __future__ import annotations

from infrastructure.persistence.repositories import (
    EventOddsSourceState,
    EventSourceMappingRepository,
)

SOFASCORE_SOURCE = "sofascore"
ODDSPAPI_SOURCE = "oddspapi"
PRE_START_ODDS_SOURCES = (SOFASCORE_SOURCE, ODDSPAPI_SOURCE)
PreStartOddsSourceStates = dict[int, dict[str, EventOddsSourceState]]


def load_pre_start_odds_source_states(
    events: list[dict],
) -> PreStartOddsSourceStates:
    """Load source IDs and endpoint availability for all upcoming events once."""
    return EventSourceMappingRepository.get_odds_source_states(
        event_ids=[event.get("id") for event in events or []],
        sources=PRE_START_ODDS_SOURCES,
    )


def get_numeric_source_event_id(
    source_states: PreStartOddsSourceStates,
    event_id: int,
    source: str,
) -> int | None:
    """Return a numeric external event ID when the provider mapping has one."""
    state = source_states.get(event_id, {}).get(source)
    if state is None:
        return None
    try:
        return int(state.source_event_id)
    except (TypeError, ValueError):
        return None
