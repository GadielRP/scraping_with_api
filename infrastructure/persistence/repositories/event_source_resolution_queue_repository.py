"""Repository for unresolved or review-needed external event resolutions."""

from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy.orm import Session

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import EventSourceResolutionQueue
from shared.timezone_utils import get_local_now
from modules.oddspapi.event_candidate_matcher import EventCandidateScore
from modules.oddspapi.fixture_normalizer import OddspapiFixtureIdentity

logger = logging.getLogger(__name__)


class EventSourceResolutionQueueRepository:
    @staticmethod
    def _normalize_source(source: str) -> str:
        return str(source or "").strip().lower()

    @staticmethod
    def _normalize_source_event_id(source_event_id) -> str:
        return str(source_event_id).strip()

    @staticmethod
    def _as_serializable_candidate_scores(
        candidate_scores: Iterable[EventCandidateScore],
    ) -> list[dict[str, object]]:
        return [
            score.to_dict() if hasattr(score, "to_dict") else dict(score)
            for score in candidate_scores
        ]

    @staticmethod
    def _candidate_value(candidate, key: str):
        if isinstance(candidate, dict):
            return candidate.get(key)
        return getattr(candidate, key, None)

    @staticmethod
    def _upsert_unresolved_attempt_in_session(
        session: Session,
        fixture: OddspapiFixtureIdentity,
        resolution_status: str,
        candidate_scores: list[EventCandidateScore],
    ) -> EventSourceResolutionQueue:
        normalized_source = EventSourceResolutionQueueRepository._normalize_source("oddspapi")
        normalized_source_event_id = EventSourceResolutionQueueRepository._normalize_source_event_id(
            fixture.fixture_id,
        )
        if not normalized_source_event_id:
            raise ValueError("fixture_id is required for event_source_resolution_queue")

        sorted_scores = sorted(
            candidate_scores,
            key=lambda item: (
                -float(EventSourceResolutionQueueRepository._candidate_value(item, "score") or 0),
                int(EventSourceResolutionQueueRepository._candidate_value(item, "event_id") or 0),
            ),
        )
        best_candidate = sorted_scores[0] if sorted_scores else None
        second_candidate = sorted_scores[1] if len(sorted_scores) > 1 else None
        score_gap = None
        if best_candidate is not None and second_candidate is not None:
            score_gap = round(
                float(EventSourceResolutionQueueRepository._candidate_value(best_candidate, "score") or 0)
                - float(EventSourceResolutionQueueRepository._candidate_value(second_candidate, "score") or 0),
                3,
            )

        queue_row = (
            session.query(EventSourceResolutionQueue)
            .filter(
                EventSourceResolutionQueue.source == normalized_source,
                EventSourceResolutionQueue.source_event_id == normalized_source_event_id,
            )
            .first()
        )

        if queue_row is None:
            queue_row = EventSourceResolutionQueue(
                source=normalized_source,
                source_event_id=normalized_source_event_id,
                resolution_status=resolution_status,
                best_candidate_event_id=EventSourceResolutionQueueRepository._candidate_value(best_candidate, "event_id") if best_candidate else None,
                best_candidate_confidence=EventSourceResolutionQueueRepository._candidate_value(best_candidate, "score") if best_candidate else None,
                second_candidate_event_id=EventSourceResolutionQueueRepository._candidate_value(second_candidate, "event_id") if second_candidate else None,
                second_candidate_confidence=EventSourceResolutionQueueRepository._candidate_value(second_candidate, "score") if second_candidate else None,
                score_gap=score_gap,
                source_sport_id=fixture.sport_id,
                source_sport_name=fixture.sport_name,
                normalized_sport=fixture.normalized_sport,
                source_tournament_id=fixture.tournament_id,
                source_tournament_name=fixture.tournament_name,
                source_tournament_slug=fixture.tournament_slug,
                source_category_name=fixture.category_name,
                source_category_slug=fixture.category_slug,
                source_season_id=fixture.season_id,
                participant1_id=fixture.participant1_id,
                participant1_name=fixture.participant1_name,
                participant1_short_name=fixture.participant1_short_name,
                participant1_abbr=fixture.participant1_abbr,
                participant2_id=fixture.participant2_id,
                participant2_name=fixture.participant2_name,
                participant2_short_name=fixture.participant2_short_name,
                participant2_abbr=fixture.participant2_abbr,
                source_start_time_utc=fixture.start_time_utc,
                raw_external_providers=dict(fixture.external_providers),
                raw_payload=dict(fixture.raw_payload),
                candidate_scores=EventSourceResolutionQueueRepository._as_serializable_candidate_scores(
                    sorted_scores,
                ),
                attempt_count=1,
            )
            session.add(queue_row)
            logger.info(
                "Created unresolved OddsPapi queue row source=%s source_event_id=%s status=%s candidates=%s",
                normalized_source,
                normalized_source_event_id,
                resolution_status,
                len(sorted_scores),
            )
        else:
            queue_row.resolution_status = resolution_status
            queue_row.best_candidate_event_id = EventSourceResolutionQueueRepository._candidate_value(best_candidate, "event_id") if best_candidate else None
            queue_row.best_candidate_confidence = EventSourceResolutionQueueRepository._candidate_value(best_candidate, "score") if best_candidate else None
            queue_row.second_candidate_event_id = EventSourceResolutionQueueRepository._candidate_value(second_candidate, "event_id") if second_candidate else None
            queue_row.second_candidate_confidence = EventSourceResolutionQueueRepository._candidate_value(second_candidate, "score") if second_candidate else None
            queue_row.score_gap = score_gap
            queue_row.source_sport_id = fixture.sport_id
            queue_row.source_sport_name = fixture.sport_name
            queue_row.normalized_sport = fixture.normalized_sport
            queue_row.source_tournament_id = fixture.tournament_id
            queue_row.source_tournament_name = fixture.tournament_name
            queue_row.source_tournament_slug = fixture.tournament_slug
            queue_row.source_category_name = fixture.category_name
            queue_row.source_category_slug = fixture.category_slug
            queue_row.source_season_id = fixture.season_id
            queue_row.participant1_id = fixture.participant1_id
            queue_row.participant1_name = fixture.participant1_name
            queue_row.participant1_short_name = fixture.participant1_short_name
            queue_row.participant1_abbr = fixture.participant1_abbr
            queue_row.participant2_id = fixture.participant2_id
            queue_row.participant2_name = fixture.participant2_name
            queue_row.participant2_short_name = fixture.participant2_short_name
            queue_row.participant2_abbr = fixture.participant2_abbr
            queue_row.source_start_time_utc = fixture.start_time_utc
            queue_row.raw_external_providers = dict(fixture.external_providers)
            queue_row.raw_payload = dict(fixture.raw_payload)
            queue_row.candidate_scores = EventSourceResolutionQueueRepository._as_serializable_candidate_scores(
                sorted_scores,
            )
            queue_row.attempt_count = (queue_row.attempt_count or 0) + 1
            logger.info(
                "Updated unresolved OddsPapi queue row source=%s source_event_id=%s status=%s attempt_count=%s candidates=%s",
                normalized_source,
                normalized_source_event_id,
                resolution_status,
                queue_row.attempt_count,
                len(sorted_scores),
            )

        queue_row.last_attempted_at = get_local_now()
        queue_row.updated_at = get_local_now()
        return queue_row

    @staticmethod
    def upsert_unresolved_attempt(
        fixture: OddspapiFixtureIdentity,
        resolution_status: str,
        candidate_scores: list[EventCandidateScore],
        session: Session | None = None,
    ) -> EventSourceResolutionQueue:
        try:
            if session is not None:
                queue_row = EventSourceResolutionQueueRepository._upsert_unresolved_attempt_in_session(
                    session=session,
                    fixture=fixture,
                    resolution_status=resolution_status,
                    candidate_scores=candidate_scores,
                )
                return queue_row

            with db_manager.get_session() as session:
                queue_row = EventSourceResolutionQueueRepository._upsert_unresolved_attempt_in_session(
                    session=session,
                    fixture=fixture,
                    resolution_status=resolution_status,
                    candidate_scores=candidate_scores,
                )
                return queue_row
        except Exception as exc:
            logger.error(
                "Error upserting unresolved OddsPapi resolution for fixture_id=%s status=%s: %s",
                getattr(fixture, "fixture_id", None),
                resolution_status,
                exc,
            )
            raise

    @staticmethod
    def clear_resolved(
        source: str,
        source_event_id: str,
        session: Session | None = None,
    ) -> int:
        normalized_source = EventSourceResolutionQueueRepository._normalize_source(source)
        normalized_source_event_id = EventSourceResolutionQueueRepository._normalize_source_event_id(source_event_id)
        if not normalized_source or not normalized_source_event_id:
            return 0

        def _delete(scoped_session: Session) -> int:
            deleted = (
                scoped_session.query(EventSourceResolutionQueue)
                .filter(
                    EventSourceResolutionQueue.source == normalized_source,
                    EventSourceResolutionQueue.source_event_id == normalized_source_event_id,
                )
                .delete(synchronize_session=False)
            )
            return deleted

        try:
            if session is not None:
                deleted = _delete(session)
                if deleted:
                    logger.info(
                        "Cleared resolved OddsPapi queue rows for source=%s source_event_id=%s",
                        normalized_source,
                        normalized_source_event_id,
                    )
                return deleted

            with db_manager.get_session() as session:
                deleted = _delete(session)
                if deleted:
                    logger.info(
                        "Cleared resolved OddsPapi queue rows for source=%s source_event_id=%s",
                        normalized_source,
                        normalized_source_event_id,
                    )
                return deleted
        except Exception as exc:
            logger.error(
                "Error clearing resolved OddsPapi queue row for source=%s source_event_id=%s: %s",
                normalized_source,
                normalized_source_event_id,
                exc,
            )
            raise
