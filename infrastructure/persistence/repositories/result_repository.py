"""Pending-result selection and bounded, conflict-safe result persistence."""

from dataclasses import dataclass
import logging
from time import monotonic

from sqlalchemy import and_, func, or_

from infrastructure.persistence.models import Event, EventSourceMapping, Result
from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config
from shared.batching import chunks

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResultCandidate:
    id: int
    sport: str
    source_event_id: str | None
    mapping_count: int = 1


class ResultRepository:
    @staticmethod
    def pending_batches(selection, read_size):
        """Keyset pages with fixed high water; each read closes before HTTP."""
        if read_size < 1:
            raise ValueError("Result read batch size must be positive")
        if selection.start is not None:
            eligible = and_(Event.starts_at >= selection.start, Event.starts_at < selection.end)
        else:
            eligible = or_(
                *(
                    and_(Event.sport == sport, Event.starts_at < cutoff)
                    for sport, cutoff in selection.sport_cutoffs.items()
                ),
                and_(
                    ~Event.sport.in_(selection.sport_cutoffs),
                    Event.starts_at < selection.default_cutoff,
                )
            )
        with db_manager.get_session() as session:
            lower_id, upper_id = (
                session.query(func.min(Event.id), func.max(Event.id)).filter(eligible).one()
            )
        if lower_id is None:
            return
        # Date ranges need not start near canonical ID 1. Avoid scanning a long
        # historical prefix to find the first page, without changing keyset semantics.
        after_id = lower_id - 1
        while after_id < upper_id:
            with db_manager.get_session() as session:
                # A canonical event may have several external mappings. Do not
                # duplicate page rows or arbitrarily choose an ambiguous identity.
                source_id = (
                    session.query(func.min(EventSourceMapping.source_event_id))
                    .filter(
                        EventSourceMapping.event_id == Event.id,
                        EventSourceMapping.source == "sofascore",
                    )
                    .having(func.count() == 1)
                    .correlate(Event)
                    .scalar_subquery()
                )
                mapping_count = (
                    session.query(func.count(EventSourceMapping.source_event_id))
                    .filter(
                        EventSourceMapping.event_id == Event.id,
                        EventSourceMapping.source == "sofascore",
                    )
                    .correlate(Event)
                    .scalar_subquery()
                )
                rows = (
                    session.query(Event.id, Event.sport, source_id, mapping_count)
                    .filter(
                        eligible,
                        Event.id > after_id,
                        Event.id <= upper_id,
                        ~session.query(Result.event_id)
                        .filter(
                            Result.event_id == Event.id,
                            Result.home_score.isnot(None),
                            Result.away_score.isnot(None),
                        )
                        .exists(),
                    )
                    .order_by(Event.id)
                    .limit(read_size)
                    .all()
                )
                batch = [ResultCandidate(*row) for row in rows]
            if not batch:
                break
            after_id = batch[-1].id
            yield batch

    @staticmethod
    def batch_upsert_results(results_data):
        """Commit bounded SQL upserts; propagate failures to the coordinator.

        Parent row locks serialize with event deletion. Events deleted before
        locking are omitted; callers compare committed and requested counts.
        """
        confirmed = set()
        fields = ("home_score", "away_score", "winner", "home_sets", "away_sets")
        for batch in chunks(results_data, max(1, Config.EVENT_WRITE_BATCH_SIZE)):
            started = monotonic()
            result_payloads = dict(batch)
            values = {
                event_id: dict(event_id=event_id, **{key: data.get(key) for key in fields})
                for event_id, data in batch
            }
            with db_manager.get_session() as session:
                parents = (
                    session.query(Event.id, Event.sport)
                    .filter(
                        Event.id.in_(values),
                    )
                    .order_by(Event.id)
                    .with_for_update()
                    .all()
                )
                sports = dict(parents)
                existing_ids = sorted(sports)
                if not existing_ids:
                    continue
                if session.get_bind().dialect.name == "postgresql":
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert
                statement = insert(Result).values([values[event_id] for event_id in existing_ids])
                committed = session.execute(
                    statement.on_conflict_do_update(
                        index_elements=["event_id"],
                        set_={key: getattr(statement.excluded, key) for key in fields},
                    ).returning(Result.event_id)
                ).all()
                from .observation_repository import ObservationRepository

                observations = []
                for (event_id,) in committed:
                    for observation in result_payloads[event_id].get("observations") or []:
                        kind, value = observation.get("type"), observation.get("value")
                        if kind and kind != "rankings" and value is not None:
                            observations.append(
                                dict(
                                    event_id=event_id,
                                    sport=observation.get("sport") or sports[event_id],
                                    observation_type=kind,
                                    observation_value=str(value),
                                )
                            )
                ObservationRepository.write_observations(session, observations)
                from .reporting_refresh_repository import invalidate_reporting

                invalidate_reporting(session)
            confirmed.update(row[0] for row in committed)
            logger.info(
                "Result batch committed requested=%s written=%s missing=%s duration_s=%.3f",
                len(values),
                len(committed),
                len(values) - len(committed),
                monotonic() - started,
            )
        return confirmed

    @staticmethod
    def get_result_by_event_id(event_id):
        with db_manager.get_session() as session:
            return session.query(Result).filter(Result.event_id == event_id).first()
