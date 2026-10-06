"""Bounded discovery scope reads and canonical admission projections."""

from __future__ import annotations

import logging
from bisect import bisect_left, bisect_right
from collections import defaultdict
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Competition, Event
from infrastructure.settings import discovery as settings
from modules.sports.catalog import configured_sport_ids, sport_display_name
from shared.temporal import utc_now
from .competition_repository import CompetitionRepository

if TYPE_CHECKING:
    from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity

logger = logging.getLogger(__name__)

def _sport_labels(sport_ids):
    labels = {sport_display_name(value).casefold() for value in sport_ids if sport_display_name(value)}
    if "ice_hockey" in sport_ids:
        labels.add("hockey")  # Historical SofaScore classifier spelling.
    return labels


class DiscoveryRepository:
    @staticmethod
    def tracked_source_competition_ids(source, competition_ids):
        with db_manager.get_session() as session:
            return CompetitionRepository.get_source_competition_id_pairs(
                session, source=source, competition_ids=competition_ids,
            )

    @staticmethod
    def admit_query(query, policy, *, competition_ids=None, now=None):
        """Apply the same canonical policy to direct links, candidates and daily odds."""
        query = query.filter(func.lower(Event.sport).in_(_sport_labels(configured_sport_ids())))
        if policy.future_only:
            current = now or utc_now()
            query = query.filter(
                Event.starts_at > current,
                Event.starts_at >= current + timedelta(minutes=policy.minimum_lead_minutes),
            )
        if competition_ids is not None:
            query = query.filter(Event.competition_id.in_(competition_ids))
        if policy.exclude_sports and policy.excluded_sports:
            query = query.filter(~func.lower(Event.sport).in_(_sport_labels(policy.excluded_sports)))
        exclusions = []
        if policy.exclude_categories:
            exclusions.extend(
                and_(func.lower(Event.sport).in_(_sport_labels({sport})), Competition.category_name == category)
                for sport, category in policy.excluded_categories
            )
        if policy.exclude_competitions and policy.excluded_sofascore_unique_tournament_ids:
            exclusions.append(Competition.source_unique_tournament_id.in_(
                policy.excluded_sofascore_unique_tournament_ids
            ))
        if exclusions:
            query = query.outerjoin(Competition, Competition.competition_id == Event.competition_id)
            # Missing competition metadata does not imply membership in a denylist.
            query = query.filter(or_(
                Competition.competition_id.is_(None),
                Competition.source != "sofascore",
                ~func.coalesce(or_(*exclusions), False),
            ))
        return query

    @classmethod
    def admitted_event_ids(cls, session, event_ids, *, policy=None, competition_ids=None):
        if not event_ids:
            return set()
        query = session.query(Event.id).filter(Event.id.in_(event_ids))
        return {row[0] for row in cls.admit_query(
            query, policy or settings.ODDSPAPI.filters, competition_ids=competition_ids,
        ).all()}


def _sport_key(value: object) -> str:
    return str(sport_display_name(value) or value or "").strip().casefold()


def _db_sport_names(sport_keys: set[str]) -> list[str]:
    names = [sport_display_name(key) or key.title() for key in sorted(sport_keys) if key]
    if "ice hockey" in sport_keys:
        names.append("Hockey")
    return names


class OddspapiCandidatePool:
    """A preloaded candidate set with O(log N) time-window lookup."""

    # Keep the batch preload aligned with OddspapiEventCandidateMatcher's
    # single-fixture candidate query window.
    TOLERANCE = timedelta(hours=1)

    def __init__(self, events: list[Event] | None = None) -> None:
        self.events_by_sport: dict[str, list[Event]] = defaultdict(list)
        self._sorted_events_by_sport: dict[str, list[Event]] = {}
        self._sorted_times_by_sport: dict[str, list[datetime]] = {}
        for event in events or []:
            sport = _sport_key(getattr(event, "sport", None))
            self.events_by_sport[sport].append(event)

        for sport, sport_events in self.events_by_sport.items():
            timed = [
                event
                for event in sport_events
                if isinstance(getattr(event, "starts_at", None), datetime)
            ]
            timed.sort(key=lambda event: event.starts_at)
            self._sorted_events_by_sport[sport] = timed
            self._sorted_times_by_sport[sport] = [event.starts_at for event in timed]

    @classmethod
    def load(
        cls,
        fixtures: list[OddspapiFixtureIdentity],
        session: Session,
        *,
        competition_ids: tuple[int, ...] | None = None,
    ) -> "OddspapiCandidatePool":
        if not fixtures:
            return cls([])

        times = [fixture.starts_at for fixture in fixtures if fixture.starts_at is not None]
        sport_keys = {
            _sport_key(fixture.normalized_sport)
            for fixture in fixtures
            if fixture.normalized_sport
        }
        query = session.query(Event).options(
            joinedload(Event.home_participant),
            joinedload(Event.away_participant),
            joinedload(Event.competition_ref),
        )
        db_sports = _db_sport_names(sport_keys)
        if db_sports or sport_keys:
            # Prefer exact canonical sport labels for index-friendly equality,
            # and keep a lower() fallback for legacy spelling variants.
            clauses = []
            if db_sports:
                clauses.append(Event.sport.in_(db_sports))
            if sport_keys:
                clauses.append(func.lower(Event.sport).in_(sorted(sport_keys)))
            query = query.filter(or_(*clauses))
        if times:
            query = query.filter(
                Event.starts_at >= min(times) - cls.TOLERANCE,
                Event.starts_at <= max(times) + cls.TOLERANCE,
            )
        query = DiscoveryRepository.admit_query(
            query, settings.ODDSPAPI.filters, competition_ids=competition_ids,
        )
        events = query.all()
        logger.info("Loaded Oddspapi candidate pool events=%s fixtures=%s", len(events), len(fixtures))
        return cls(events)

    def get_candidates_for(self, fixture: OddspapiFixtureIdentity) -> list[Event]:
        sport = _sport_key(fixture.normalized_sport)
        fixture_time = fixture.starts_at
        if fixture_time is None:
            return list(self.events_by_sport.get(sport, []))

        times = self._sorted_times_by_sport.get(sport) or []
        events = self._sorted_events_by_sport.get(sport) or []
        if not times:
            return []

        window_start = fixture_time - self.TOLERANCE
        window_end = fixture_time + self.TOLERANCE
        left = bisect_left(times, window_start)
        right = bisect_right(times, window_end)
        return events[left:right]


