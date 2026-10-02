"""Session-owned discard storage. No provider calls or independent commits."""
from datetime import timedelta
from sqlalchemy import tuple_
from infrastructure.persistence.discard_models import EventDiscardMemory
from shared.temporal import utc_now


class EventDiscardRepository:
    @staticmethod
    def blocked_ids(session, source, ids, settings):
        if not settings.enabled or not settings.kinds or not ids:
            return set()
        return {row[0] for row in session.query(EventDiscardMemory.source_event_id).filter(
            EventDiscardMemory.source == source,
            EventDiscardMemory.source_event_id.in_(ids),
            EventDiscardMemory.parser_kind.in_(settings.kinds),
        ).all()}

    @staticmethod
    def remember(session, evidence_by_id, settings) -> int:
        """Return inserted rows; the transaction owner logs success after commit."""
        rows = [dict(source=e.source, source_event_id=e.source_event_id,
                     original_event_id=eid, parser_kind=e.parser_kind,
                     deletion_reason=e.reason, snapshot=e.snapshot, observed_at=e.observed_at,
                     discarded_at=utc_now(), origin=e.origin, policy_version=1)
                for eid, e in evidence_by_id.items()
                if settings.enabled and e.parser_kind in settings.kinds]
        if not rows:
            return 0
        if session.get_bind().dialect.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.sqlite import insert
        statement = insert(EventDiscardMemory).values(rows)
        # A repeated observation must not extend the original retention period.
        inserted = session.execute(statement.on_conflict_do_nothing(
            index_elements=['source', 'source_event_id']
        ).returning(EventDiscardMemory.source_event_id)).all()
        return len(inserted)  # Count actual inserts, including ON CONFLICT omissions.

    @staticmethod
    def cleanup(session, settings, *, now=None):
        if not settings.cleanup_enabled:
            return 0
        cutoff = (now or utc_now()) - timedelta(days=settings.retention_days)
        rows = session.query(EventDiscardMemory.source, EventDiscardMemory.source_event_id).filter(
            EventDiscardMemory.discarded_at <= cutoff
        ).order_by(EventDiscardMemory.discarded_at).limit(settings.cleanup_batch_size).with_for_update(
            skip_locked=True
        ).all()
        if not rows:
            return 0
        return session.query(EventDiscardMemory).filter(
            tuple_(EventDiscardMemory.source, EventDiscardMemory.source_event_id).in_(rows),
            EventDiscardMemory.discarded_at <= cutoff,
        ).delete(synchronize_session=False)
