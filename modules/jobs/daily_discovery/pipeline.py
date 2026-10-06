"""Commit bounded event batches, then join streamed odds to confirmed run members."""

import logging

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping
from infrastructure.persistence.repositories.daily_discovery_repository import (
    DailyDiscoveryRepository,
)
from infrastructure.persistence.repositories.discovery_repository import DiscoveryRepository
from infrastructure.persistence.transient.discovery_run_store import DiscoveryRunStore
from infrastructure.runtime.resource_budget import check_maintenance_capacity
from infrastructure.settings import discovery as settings
from infrastructure.settings.job_execution import JobExecutionSettings
from modules.competition.tracked_competitions import tracked_competition_ids
from modules.jobs.discovery.filters import load_tracked_source_competitions, sofascore_discovery_sport_slugs
from modules.jobs.discovery.summary import log_discovery_summary
from modules.odds_ingestion import MarketOddsIngestionService
from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter
from modules.sofascore import api_client
from infrastructure.network.json_document import document_entries
from shared.batching import chunks
from shared.execution_context import WorkDeferred, check_execution_budget

from .event_source import iter_sport_events
from .persistence import persist_daily_events

logger = logging.getLogger(__name__)


def persist_sport_odds(client, date, sport, store, batch_size):
    saved = 0
    with client.open_scheduled_odds(date, sport) as document:
        for batch in chunks(document_entries(document, "odds", mapping=True), batch_size):
            check_execution_budget()
            check_maintenance_capacity()
            members = store.source_members([sid for sid, _ in batch])
            if not members:
                continue
            # Recheck live canonical identity and kickoff in one short read transaction.
            with db_manager.get_session() as session:
                query = (
                    session.query(EventSourceMapping.source_event_id, Event.id)
                    .join(Event, Event.id == EventSourceMapping.event_id)
                    .where(
                        Event.id.in_(members.values()),
                        EventSourceMapping.source == "sofascore",
                        EventSourceMapping.source_event_id.in_(members),
                    )
                )
                policy = settings.SOFASCORE.filters
                eligible = dict(DiscoveryRepository.admit_query(
                    query, policy,
                    competition_ids=tracked_competition_ids() if policy.tracked_competitions_only else None,
                ).all())
            for sid, raw in batch:
                event_id = eligible.get(str(sid))
                if event_id is None or event_id != members.get(str(sid)):
                    continue
                odds = SofaScoreMarketAdapter.from_daily_odds_entry(raw)
                if odds.get("markets"):
                    result = MarketOddsIngestionService.save_from_sofascore_response(
                        event_id, odds, source="sofascore"
                    )
                    saved += int(result.markets_saved > 0 or result.dual_process_market_available)
    return saved


def discover_events_for_date(date, sports=None, run_slot=None, *, client=api_client):
    if run_slot not in {"current_utc_day", "next_utc_day"}:
        raise ValueError("Daily discovery requires an explicit UTC-date slot")
    limits = JobExecutionSettings()
    scope = load_tracked_source_competitions("sofascore")
    if scope is not None and not scope:
        raise ValueError("Configured SofaScore discovery scope has no provider mappings")
    stats = dict.fromkeys(
        (
            "events_processed",
            "events_persisted",
            "events_inserted",
            "events_updated",
            "events_discarded",
            "events_failed",
            "events_filtered",
            "sports_failed",
            "odds_inserted",
        ),
        0,
    )
    with DiscoveryRunStore() as store:
        try:
            for sport in sofascore_discovery_sport_slugs(sports):
                check_execution_budget()
                failed = False
                try:
                    for batch in chunks(
                        iter_sport_events(client, date, sport, scope, store),
                        limits.event_read_batch_size,
                    ):
                        check_execution_budget()
                        check_maintenance_capacity()
                        result = persist_daily_events(
                            client, batch, run_store=store, tracked_competitions=scope,
                        )
                        stats["events_processed"] += len(batch)
                        for name, value in (
                            ("persisted", result.persisted),
                            ("inserted", result.inserted),
                            ("updated", result.updated),
                            ("discarded", result.discarded),
                            ("failed", result.failed),
                            ("filtered", result.filtered),
                        ):
                            stats[f"events_{name}"] += value
                        failed |= result.failed > 0
                    stats["odds_inserted"] += persist_sport_odds(
                        client, date, sport, store, limits.event_read_batch_size
                    )
                except (KeyboardInterrupt, WorkDeferred):
                    DailyDiscoveryRepository.update_sport_status(date, run_slot, sport, "failed")
                    raise
                except Exception:
                    failed = True
                    logger.exception(
                        "Daily sport incomplete date=%s slot=%s sport=%s", date, run_slot, sport
                    )
                stats["sports_failed"] += int(failed)
                DailyDiscoveryRepository.update_sport_status(
                    date, run_slot, sport, "failed" if failed else "completed"
                )
            logger.info("Daily discovery persistence summary: %s", stats)
            return stats
        finally:
            log_discovery_summary(
                store, logger, job="daily_discovery", requested_date=date, run_slot=run_slot
            )
