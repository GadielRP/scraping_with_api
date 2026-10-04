"""Atomic event removal and optional evidence, bounded by write batch size."""
import logging
from collections import Counter
from time import monotonic
from infrastructure.persistence.models import Event, EventSourceMapping, Result, EventObservation, Season
from infrastructure.persistence.event_identity_lock import lock_event_identities
from modules.events.discards.settings import DiscardSettings
from .event_discard_repository import EventDiscardRepository
from shared.batching import chunks

logger = logging.getLogger(__name__)


def delete_events(db_manager, event_ids):
    settings = DiscardSettings.current()
    evidence = getattr(event_ids, 'evidence', {})
    total = 0
    for ids in chunks(sorted(set(event_ids)), settings.batch_size):
        started = monotonic()
        with db_manager.get_session() as session:
            identities = session.query(EventSourceMapping.source, EventSourceMapping.source_event_id).filter(
                EventSourceMapping.event_id.in_(ids)).all()
            lock_event_identities(session, identities)
            events = session.query(Event).filter(Event.id.in_(ids)).order_by(Event.id).with_for_update().all()
            current_mappings = {(m.source, m.source_event_id): m.event_id for m in session.query(EventSourceMapping).filter(
                EventSourceMapping.event_id.in_(ids)).all()}
            result_ids = {r[0] for r in session.query(Result.event_id).filter(Result.event_id.in_(ids)).all()}
            selected = []
            for event in events:
                proof = evidence.get(event.id)
                if proof and (current_mappings.get((proof.source, proof.source_event_id)) != event.id
                              or event.id in result_ids
                              or (event.updated_at and event.updated_at > proof.observed_at)):
                    logger.warning('Discard conflict event_id=%s; newer data or identity changed', event.id)
                    continue
                selected.append(event)
            deleted_ids = [e.id for e in selected]
            if not deleted_ids:
                continue
            proofs = {eid: evidence[eid] for eid in deleted_ids if eid in evidence}
            eligible = [p for p in proofs.values() if settings.enabled and p.parser_kind in settings.kinds]
            memorized = EventDiscardRepository.remember(session, proofs, settings)
            session.query(Result).filter(Result.event_id.in_(deleted_ids)).delete(synchronize_session=False)
            session.query(EventObservation).filter(EventObservation.event_id.in_(deleted_ids)).delete(synchronize_session=False)
            # Explicit for SQLite tests and non-cascading legacy schemas too.
            session.query(EventSourceMapping).filter(EventSourceMapping.event_id.in_(deleted_ids)).delete(synchronize_session=False)
            count = session.query(Event).filter(Event.id.in_(deleted_ids)).delete(synchronize_session=False)
            from .reporting_refresh_repository import invalidate_reporting
            invalidate_reporting(session)
            seasons = {e.season_id for e in selected if e.season_id is not None}
            if seasons:
                remaining = session.query(Event.season_id).filter(Event.season_id.in_(seasons))
                session.query(Season).filter(Season.id.in_(seasons), ~Season.id.in_(remaining)).delete(synchronize_session=False)
        total += count
        logger.info(
            'Batch deletion committed: deleted=%s memory_eligible=%s memory_inserted=%s '
            'memory_existing=%s deleted_without_memory=%s memory_kinds=%s '
            'conflicts=%s missing=%s duration_s=%.3f deleted_event_ids=%s',
            count, len(eligible), memorized, len(eligible) - memorized,
            count - len(eligible), dict(Counter(p.parser_kind for p in eligible)),
            len(events) - len(selected), len(ids) - len(events), monotonic() - started, deleted_ids,
        )
    return total
