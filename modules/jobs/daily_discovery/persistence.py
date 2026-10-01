"""Daily batch boundary; event transactions contain no HTTP work."""
from dataclasses import dataclass
import logging
from infrastructure.persistence.repositories import EventRepository
from infrastructure.persistence.repositories.event_batch_writer import chunks
from modules.competition.discovery_scope import is_tracked_source_event, load_tracked_source_competitions, UNRESOLVED_SCOPE
from modules.events.discards.settings import DiscardSettings
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sports.catalog import is_supported_sofascore_event
from shared.shutdown import is_shutdown_requested

logger = logging.getLogger(__name__)


@dataclass
class DiscoveryWriteSummary:
    persisted: int = 0
    odds_saved: int = 0
    discarded: int = 0
    out_of_scope: int = 0
    failed: int = 0


def persist_events_and_optional_odds(api_client, events, odds_map=None, *, tracked_competitions=UNRESOLVED_SCOPE):
    if tracked_competitions is UNRESOLVED_SCOPE:
        tracked_competitions = load_tracked_source_competitions('sofascore')
    summary = DiscoveryWriteSummary()
    odds_map = odds_map or {}
    for batch in chunks(events, DiscardSettings.current().batch_size):
        if is_shutdown_requested():
            raise KeyboardInterrupt()
        eligible = []
        for raw in batch:
            if not raw.get('id'):
                summary.failed += 1
            elif not is_supported_sofascore_event(raw) or not is_tracked_source_event(raw, tracked_competitions):
                summary.out_of_scope += 1
            else:
                eligible.append(raw)
        blocked = EventRepository.discarded_source_ids('sofascore', [e['id'] for e in eligible])
        normalized = []
        for raw in eligible:
            if str(raw['id']) in blocked:
                summary.discarded += 1
                continue
            try:
                data = api_client.normalize_event_payload(raw, discovery_source='daily_discovery')
                if not data or not is_supported_sofascore_event(data):
                    raise ValueError('Invalid normalized event')
                normalized.append(data)
            except Exception:
                logger.exception('Cannot normalize daily event %s', raw['id'])
                summary.failed += 1
        result = EventRepository.batch_upsert_events(normalized)
        summary.persisted += len(result.events)
        summary.discarded += len(result.discarded)
        summary.failed += len(result.errors)
        for sid, event in result.events.items():
            odds = odds_map.get(sid) or odds_map.get(int(sid))
            if not odds:
                continue
            try:
                saved = MarketOddsIngestionService.save_from_sofascore_response(event.id, odds, source='sofascore')
                if saved.markets_saved <= 0 and not saved.dual_process_market_available:
                    summary.failed += 1
                else:
                    summary.odds_saved += 1
            except Exception:
                logger.exception('Cannot persist daily odds source_event_id=%s', sid)
                summary.failed += 1
    return summary


def persist_event_and_optional_odds(api_client, event, odds_data=None, *, tracked_competitions=UNRESOLVED_SCOPE):
    """Single-item compatibility; discarded identities are a successful omission."""
    result = persist_events_and_optional_odds(api_client, [event], {str(event.get('id')): odds_data},
                                             tracked_competitions=tracked_competitions)
    return result.failed == 0 and (result.persisted + result.discarded) > 0
