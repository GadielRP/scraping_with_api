"""Discovery admission: provider scopes, time, rankings and curated exclusions."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime, timedelta

from infrastructure.settings import discovery as settings
from infrastructure.settings.discovery import DiscoveryFilters
from modules.competition.tracked_competitions import tracked_competition_ids
from modules.sports.catalog import (
    canonical_sport_id,
    configured_sport_ids,
    oddspapi_sport_id_for_fixture,
    sofascore_sport_id_for_event,
)
from shared.temporal import UTC, utc_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SourceCompetitionIds:
    """Provider tournament IDs stored on one canonical competition row."""

    source_tournament_id: int | None
    source_unique_tournament_id: int | None


def load_tracked_source_competitions(
    source: str,
) -> frozenset[SourceCompetitionIds] | None:
    """Load provider IDs, or return None when competition filtering is disabled."""
    policy = settings.SOFASCORE.filters if source == "sofascore" else settings.ODDSPAPI.filters
    if not policy.tracked_competitions_only:
        return None
    from infrastructure.persistence.repositories.discovery_repository import DiscoveryRepository

    return frozenset(
        SourceCompetitionIds(*ids)
        for ids in DiscoveryRepository.tracked_source_competition_ids(source, tracked_competition_ids())
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
    if not isinstance(tournament, dict):
        tournament = {}
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


# Omitted scope loads configuration; explicit None means filtering is disabled.
UNRESOLVED_SCOPE = object()


def exclusion_reason(sport_id, category_name, unique_tournament_id, policy: DiscoveryFilters,
                     *, include_sport=True) -> str | None:
    """Evaluate curated exclusions without reading availability or querying the DB."""
    if include_sport and policy.exclude_sports and sport_id in policy.excluded_sports:
        return "excluded_sport"
    if policy.exclude_categories and (sport_id, category_name) in policy.excluded_categories:
        return "excluded_category"
    if (policy.exclude_competitions
        and unique_tournament_id in policy.excluded_sofascore_unique_tournament_ids):
        return "excluded_competition"
    return None


def start_time_filter_reason(starts_at: datetime | None, policy: DiscoveryFilters,
                             *, now: datetime | None = None) -> str | None:
    if not policy.future_only:
        return None
    if starts_at is None:
        return "missing_start_timestamp"
    current = now or utc_now()
    if starts_at <= current or starts_at < current + timedelta(minutes=policy.minimum_lead_minutes):
        return "start_too_soon_or_started"
    return None


def _category_name(event):
    payload = event.get("event", event)
    ref = payload.get("competition_ref") or event.get("competition_ref") or {}
    if isinstance(ref, dict) and ref.get("category_name"):
        return ref["category_name"]
    tournament = payload.get("tournament") or {}
    category = tournament.get("category") or {} if isinstance(tournament, dict) else {}
    return category.get("name") if isinstance(category, dict) else None


def sofascore_tournament_filter_reason(event, scope, sport, *, policy=None):
    """Reject calendar tournaments before downloading their scheduled events."""
    policy = policy or settings.SOFASCORE.filters
    reason = source_competition_filter_reason(event, scope)
    if reason:
        return reason
    return exclusion_reason(
        canonical_sport_id(sport), _category_name(event),
        source_competition_ids(event).source_unique_tournament_id, policy,
        # Singles and doubles share the tennis calendar; only event metadata
        # can distinguish their sport. Request routes already exclude other sports.
        include_sport=False,
    )


def _participant_ranking(participant) -> int | None:
    """Prefer the current player ranking, falling back to the event's ranking."""
    if not isinstance(participant, dict):
        return None
    player_info = participant.get("playerTeamInfo") or {}
    current_ranking = player_info.get("currentRanking") if isinstance(player_info, dict) else None
    for value in (current_ranking, participant.get("ranking")):
        ranking = _as_int(value)
        if ranking is not None and ranking > 0:
            return ranking
    return None


def sofascore_event_filter_reason(event, scope=None, *, policy=None, now=None):
    """Evaluate raw or normalized data; participant rankings require raw data."""
    policy = policy or settings.SOFASCORE.filters
    if not isinstance(event, dict) or not isinstance(event.get("event", event), dict):
        return "invalid_event_payload"
    sport_id = sofascore_sport_id_for_event(event)
    if sport_id not in configured_sport_ids():
        return "unsupported_sport"
    reason = source_competition_filter_reason(event, scope) or exclusion_reason(
        sport_id, _category_name(event),
        source_competition_ids(event).source_unique_tournament_id, policy,
    )
    if reason:
        return reason
    payload = event.get("event", event)
    if settings.SOFASCORE.tennis_ranking_filter_enabled and sport_id in ("tennis", "tennis_doubles"):
        for side in ("homeTeam", "awayTeam"):
            ranking = _participant_ranking(payload.get(side))
            if ranking is not None and ranking >= settings.SOFASCORE.tennis_ranking_cutoff:
                return "tennis_ranking_excluded"
    if not policy.future_only:
        return None
    raw_start = payload.get("startTimestamp")
    if raw_start is None:
        return "missing_start_timestamp"
    try:
        starts_at = datetime.fromtimestamp(int(raw_start), tz=UTC)
    except (TypeError, ValueError, OverflowError, OSError):
        return "invalid_start_timestamp"
    return start_time_filter_reason(starts_at, policy, now=now)


def filter_sofascore_events(events, scope=None, *, policy=None, now=None):
    """Filter one bounded batch, recording rejected counts rather than payloads."""
    current = now or utc_now()
    counts = Counter()
    eligible = []
    for event in events or ():
        reason = sofascore_event_filter_reason(event, scope, policy=policy, now=current)
        counts[reason or "accepted"] += 1
        if reason is None:
            eligible.append(event)
    logger.info("Discovery admission source=sofascore counts=%s", dict(counts))
    return eligible


def oddspapi_fixture_filter_reason(fixture, *, policy=None, now=None):
    """Evaluate provider metadata before identity resolution or queue writes."""
    policy = policy or settings.ODDSPAPI.filters
    sport_id = oddspapi_sport_id_for_fixture(fixture)
    if sport_id not in configured_sport_ids():
        return "unsupported_sport"
    reason = exclusion_reason(sport_id, None, None, policy)
    if reason or not policy.future_only:
        return reason
    raw_start = fixture.get("startTime")
    if not raw_start:
        return "missing_start_timestamp"
    try:
        starts_at = datetime.fromisoformat(str(raw_start).replace("Z", "+00:00"))
        starts_at = starts_at.replace(tzinfo=UTC) if starts_at.tzinfo is None else starts_at.astimezone(UTC)
    except (TypeError, ValueError):
        return "invalid_start_timestamp"
    return start_time_filter_reason(starts_at, policy, now=now)


def sofascore_discovery_sport_routes(requested_sports=None):
    from modules.sports.catalog import sofascore_sport_routes

    policy = settings.SOFASCORE.filters
    return [
        (sport, slug) for sport, slug in sofascore_sport_routes(requested_sports)
        if not policy.exclude_sports or sport not in policy.excluded_sports
    ]


def sofascore_discovery_sport_slugs(requested_sports=None):
    return list(dict.fromkeys(slug for _, slug in sofascore_discovery_sport_routes(requested_sports)))


def oddspapi_discovery_sport_ids(requested_sports=None):
    from modules.sports.catalog import oddspapi_sport_ids

    policy = settings.ODDSPAPI.filters
    requested = configured_sport_ids(requested_sports) if requested_sports is not None else configured_sport_ids()
    if policy.exclude_sports:
        requested = requested - policy.excluded_sports
    return oddspapi_sport_ids(requested)
