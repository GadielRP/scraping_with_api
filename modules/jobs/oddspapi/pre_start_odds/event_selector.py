"""Pure selection of pre-start events eligible for Oddspapi odds requests."""

from __future__ import annotations

from dataclasses import dataclass

from infrastructure.persistence.repositories import EventOddsSourceState

from .constants import ODDSPAPI_SOURCE


@dataclass(frozen=True)
class OddspapiPreStartCandidate:
    """Minimum data required to request one Oddspapi odds endpoint."""

    event_id: int
    fixture_id: str | None
    minutes_until_start: int | float | None
    has_odds: bool = True
    source_sport_id: str | None = None


def _canonical_event_id(event_info: dict) -> int | None:
    value = event_info.get("event_id")
    if value is None:
        value = (event_info.get("event_data") or {}).get("id")
    if isinstance(value, bool):
        return None
    try:
        event_id = int(value)
    except (TypeError, ValueError):
        return None
    return event_id if event_id > 0 else None


def select_oddspapi_pre_start_candidates(
    events_to_process: list[dict],
    source_states: dict[int, dict[str, EventOddsSourceState]] | None = None,
) -> list[OddspapiPreStartCandidate]:
    """Select events using the timing decision made by the main orchestrator."""
    candidates: list[OddspapiPreStartCandidate] = []
    for event_info in events_to_process or []:
        if event_info.get("should_extract_odds") is not True:
            continue
        event_id = _canonical_event_id(event_info)
        if event_id is None:
            continue
        source_state = (source_states or {}).get(event_id, {}).get(ODDSPAPI_SOURCE)
        candidates.append(
            OddspapiPreStartCandidate(
                event_id=event_id,
                fixture_id=source_state.source_event_id if source_state else None,
                minutes_until_start=event_info.get("minutes_until_start"),
                has_odds=source_state.has_odds if source_state else True,
                source_sport_id=source_state.source_sport_id if source_state else None,
            )
        )
    return candidates
