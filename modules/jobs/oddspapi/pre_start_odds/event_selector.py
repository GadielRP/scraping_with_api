"""Pure selection of pre-start events eligible for Oddspapi odds requests."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OddspapiPreStartCandidate:
    event_id: int
    fixture_id: str | None
    event_data: dict
    minutes_until_start: int | float | None
    should_extract_odds: bool
    metadata_snapshot: dict | None = None


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
) -> list[OddspapiPreStartCandidate]:
    """Select events using the timing decision made by the main orchestrator."""
    candidates: list[OddspapiPreStartCandidate] = []
    for event_info in events_to_process or []:
        if event_info.get("should_extract_odds") is not True:
            continue
        event_id = _canonical_event_id(event_info)
        if event_id is None:
            continue
        event_data = event_info.get("event_data") or {}
        candidates.append(
            OddspapiPreStartCandidate(
                event_id=event_id,
                fixture_id=None,
                event_data=event_data,
                minutes_until_start=event_info.get("minutes_until_start"),
                should_extract_odds=True,
                metadata_snapshot=event_info.get("metadata_snapshot"),
            )
        )
    return candidates
