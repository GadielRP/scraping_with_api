"""Pending-result selection and bounded, conflict-safe result persistence."""
from dataclasses import dataclass
from datetime import timedelta
import logging
from time import monotonic

from sqlalchemy import and_, func, or_

from infrastructure.persistence.models import Event, EventSourceMapping, Result
from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config
from shared.batching import chunks
from shared.temporal import local_day_bounds_utc, utc_now

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResultCandidate:
    id: int
    sport: str
    source_event_id: str | None


class ResultRepository:
    @staticmethod
    def pending_batches(target_date=None):
        """Yield keyset pages, closing each transaction before provider requests.

        The high-water mark bounds the run. Failed events are visited once and
        remain pending on retry. Confirmed results are excluded automatically.
        None retains the all-finished sport-duration policy.
        """
        size = max(1, Config.EVENT_WRITE_BATCH_SIZE)
        if target_date is not None:
            start, end = local_day_bounds_utc(target_date, Config.TIMEZONE)
            eligible = and_(Event.starts_at >= start, Event.starts_at < end)
        else:
            now = utc_now()
            eligible = or_(
                and_(Event.sport.in_(['Football', 'Futsal']), Event.starts_at < now - timedelta(hours=2.5)),
                and_(Event.sport.in_(['Tennis', 'Baseball']), Event.starts_at < now - timedelta(hours=4)),
                and_(~Event.sport.in_(['Football', 'Futsal', 'Tennis', 'Baseball']),
                     Event.starts_at < now - timedelta(hours=3)),
            )
        with db_manager.get_session() as session:
            upper_id = session.query(func.max(Event.id)).scalar() or 0
        after_id = 0
        while after_id < upper_id:
            with db_manager.get_session() as session:
                # A canonical event may have several external mappings. Do not
                # duplicate page rows or arbitrarily choose an ambiguous identity.
                source_id = session.query(func.min(EventSourceMapping.source_event_id)).filter(
                    EventSourceMapping.event_id == Event.id,
                    EventSourceMapping.source == 'sofascore',
                ).having(func.count() == 1).correlate(Event).scalar_subquery()
                rows = session.query(Event.id, Event.sport, source_id).filter(
                    eligible, Event.id > after_id, Event.id <= upper_id,
                    ~session.query(Result.event_id).filter(Result.event_id == Event.id).exists(),
                ).order_by(Event.id).limit(size).all()
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
        total = 0
        fields = ('home_score', 'away_score', 'winner', 'home_sets', 'away_sets')
        for batch in chunks(results_data, max(1, Config.EVENT_WRITE_BATCH_SIZE)):
            started = monotonic()
            values = {event_id: dict(event_id=event_id, **{key: data.get(key) for key in fields})
                      for event_id, data in batch}
            with db_manager.get_session() as session:
                existing_ids = [row[0] for row in session.query(Event.id).filter(
                    Event.id.in_(values),
                ).order_by(Event.id).with_for_update().all()]
                if not existing_ids:
                    continue
                if session.get_bind().dialect.name == 'postgresql':
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert
                statement = insert(Result).values([values[event_id] for event_id in existing_ids])
                committed = session.execute(statement.on_conflict_do_update(
                    index_elements=['event_id'],
                    set_={key: getattr(statement.excluded, key) for key in fields},
                ).returning(Result.event_id)).all()
            total += len(committed)
            logger.info('Result batch committed requested=%s written=%s missing=%s duration_s=%.3f',
                        len(values), len(committed), len(values) - len(committed), monotonic() - started)
        return total

    @staticmethod
    def get_result_by_event_id(event_id):
        with db_manager.get_session() as session:
            return session.query(Result).filter(Result.event_id == event_id).first()
