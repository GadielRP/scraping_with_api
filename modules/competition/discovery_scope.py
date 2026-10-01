"""Provider ID scopes for discovery jobs restricted to tracked competitions."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.competition_repository import CompetitionRepository
from infrastructure.settings import Config

from .tracked_competitions import tracked_competition_ids


@dataclass(frozen=True, slots=True)
class SourceCompetitionIds:
    """Provider tournament IDs stored on one canonical competition row."""

    source_tournament_id: int | None
    source_unique_tournament_id: int | None


def load_tracked_source_competitions(
    source: str,
) -> frozenset[SourceCompetitionIds] | None:
    """Load provider IDs, or return None when competition filtering is disabled."""
    if not Config.DISCOVERY_TRACKED_COMPETITIONS_ONLY:
        return None
    return load_source_competitions(source)


def load_source_competitions(source: str) -> frozenset[SourceCompetitionIds]:
    """Load provider IDs for tracked competitions, even when filtering is disabled."""
    with db_manager.get_session() as session:
        return frozenset(
            SourceCompetitionIds(*ids)
            for ids in CompetitionRepository.get_source_competition_id_pairs(
                session, source=source, competition_ids=tracked_competition_ids()
            )
        )


def source_competition_ids(event: dict | None) -> SourceCompetitionIds:
    """Read provider tournament IDs from raw or normalized event data."""
    if not isinstance(event, dict):
        return SourceCompetitionIds(None, None)
    payload = event.get("event", event)
    if not isinstance(payload, dict):
        return SourceCompetitionIds(None, None)
    competition_ref = payload.get("competition_ref") or event.get("competition_ref") or {}
    tournament = payload.get("tournament") or {}
    unique_tournament = (
        tournament.get("uniqueTournament") or {} if isinstance(tournament, dict) else {}
    )
    if not isinstance(competition_ref, dict):
        competition_ref = {}
    if not isinstance(unique_tournament, dict):
        unique_tournament = {}
    return SourceCompetitionIds(
        _as_int(
            competition_ref.get("source_tournament_id") or tournament.get("id")
        ),
        _as_int(
            competition_ref.get("source_unique_tournament_id")
            or unique_tournament.get("id")
        ),
    )


def _as_int(value) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def is_tracked_source_event(
    event: dict | None,
    tracked_competitions: Collection[SourceCompetitionIds] | None,
) -> bool:
    return source_competition_filter_reason(event, tracked_competitions) is None


def source_competition_filter_reason(
    event: dict | None,
    tracked_competitions: Collection[SourceCompetitionIds] | None,
) -> str | None:
    """Explain why an event falls outside an enabled tracked competition scope."""
    if tracked_competitions is None:
        return None
    event_ids = source_competition_ids(event)
    if (
        event_ids.source_tournament_id is None
        and event_ids.source_unique_tournament_id is None
    ):
        return "missing_source_competition_ids"

    for tracked_ids in tracked_competitions:
        comparisons = []
        if (
            event_ids.source_tournament_id is not None
            and tracked_ids.source_tournament_id is not None
        ):
            comparisons.append(event_ids.source_tournament_id == tracked_ids.source_tournament_id)
        if (
            event_ids.source_unique_tournament_id is not None
            and tracked_ids.source_unique_tournament_id is not None
        ):
            comparisons.append(
                event_ids.source_unique_tournament_id == tracked_ids.source_unique_tournament_id
            )
        if comparisons and all(comparisons):
            return None
    return "untracked_competition"


def filter_tracked_source_events(
    events,
    tracked_competitions: Collection[SourceCompetitionIds] | None,
) -> list[dict]:
    return [
        event
        for event in events or ()
        if is_tracked_source_event(event, tracked_competitions)
    ]

# Omitted scope loads configuration; explicit None means filtering is disabled.
UNRESOLVED_SCOPE = object()
