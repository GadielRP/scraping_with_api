"""Commit bounded event batches, then join streamed odds to confirmed run members."""

import logging
from collections import Counter

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
from modules.jobs.discovery.summary import (
    log_daily_filter_summary, log_discovery_configuration, log_discovery_summary,
)
from modules.odds_ingestion import MarketOddsIngestionService
from modules.odds_ingestion.adapters.sofascore_market_adapter import SofaScoreMarketAdapter
from modules.sofascore import api_client
from infrastructure.network.json_document import document_entries
from shared.batching import chunks
from shared.execution_context import WorkDeferred, check_execution_budget

from .event_source import iter_sport_events
from .observability import DailySourceCounts, log_daily_batch
from .persistence import DailyWriteSummary, persist_daily_events

logger = logging.getLogger(__name__)


def persist_sport_odds(client, date, sport, store, batch_size):
    saved = 0
    with client.open_scheduled_odds(date, sport) as document:
        # SofaScore represents the empty odds map as [] rather than {}.
        for batch in chunks(document_entries(
            document, "odds", mapping=True, allow_empty_array=True,
        ), batch_size):
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
    if run_slot not in {"anticipada", "actualizacion"}:
        raise ValueError("Daily discovery requires an explicit pass")
    limits = JobExecutionSettings()
    scope = load_tracked_source_competitions("sofascore")
    log_discovery_configuration(logger, job="daily_discovery", tracked_competitions=scope)
    if scope is not None and not scope:
        raise ValueError("Configured SofaScore discovery scope has no provider mappings")
    stats = dict.fromkeys(
        (
            "events_processed",
            "events_persisted",
            "events_inserted",
            "events_updated",
            "events_blocked_by_discard_memory",
            "events_failed",
            "events_filtered",
            "events_filtered_before_write",
            "sports_failed",
            "odds_inserted",
        ),
        0,
    )
    total_source_counts = DailySourceCounts()
    write_filter_reasons = Counter()
    with DiscoveryRunStore() as store:
        try:
            for sport in sofascore_discovery_sport_slugs(sports):
                check_execution_budget()
                failed = False
                source_counts = DailySourceCounts()
                reported_counts = DailySourceCounts()
                batch_number = 0
                source = iter_sport_events(client, date, sport, scope, store, source_counts)
                try:
                    for batch in chunks(source, limits.event_read_batch_size):
                        check_execution_budget()
                        check_maintenance_capacity()
                        result = persist_daily_events(client, batch, run_store=store)
                        batch_number += 1
                        log_daily_batch(
                            logger, source_counts.since(reported_counts), result,
                            date=date, run_slot=run_slot, sport=sport, batch_number=batch_number,
                        )
                        reported_counts = source_counts.snapshot()
                        stats["events_processed"] += len(batch)
                        for name, value in (
                            ("persisted", result.persisted),
                            ("inserted", result.inserted),
                            ("updated", result.updated),
                            ("blocked_by_discard_memory", result.blocked_by_discard_memory),
                            ("failed", result.failed),
                            ("filtered_before_write", result.filtered),
                        ):
                            stats[f"events_{name}"] += value
                        write_filter_reasons.update(result.filter_reasons)
                        failed |= result.failed > 0
                    stats["odds_inserted"] += persist_sport_odds(
                        client, date, sport, store, limits.event_read_batch_size
                    )
                except (KeyboardInterrupt, WorkDeferred):
                    stats["sports_failed"] += 1
                    DailyDiscoveryRepository.update_sport_status(date, run_slot, sport, "failed")
                    raise
                except Exception:
                    failed = True
                    logger.exception(
                        "Daily sport incomplete date=%s slot=%s sport=%s", date, run_slot, sport
                    )
                finally:
                    source.close()
                    remaining = source_counts.since(reported_counts)
                    if (remaining.totals or remaining.event_filter_reasons
                        or remaining.tournament_filter_reasons or remaining.tournament_skip_reasons):
                        log_daily_batch(
                            logger, remaining, DailyWriteSummary(), date=date,
                            run_slot=run_slot, sport=sport, batch_number=batch_number + 1,
                            phase="incomplete" if remaining.totals["events_admitted"] else "admission",
                        )
                    total_source_counts.add(source_counts)
                stats["sports_failed"] += int(failed)
                DailyDiscoveryRepository.update_sport_status(
                    date, run_slot, sport, "failed" if failed else "completed"
                )
            return stats
        finally:
            log_discovery_summary(
                store, logger, job="daily_discovery", requested_date=date, run_slot=run_slot
            )
            stats.update(total_source_counts.log_fields())
            stats["events_filtered"] = (
                stats["events_filtered_at_source"] + stats["events_filtered_before_write"]
            )
            stats["event_filter_reasons"] = dict(
                total_source_counts.event_filter_reasons + write_filter_reasons
            )
            stats["write_filter_reasons"] = dict(write_filter_reasons)
            stats["events_unprocessed"] = stats["events_admitted"] - stats["events_processed"]
            log_daily_filter_summary(logger, stats, date=date, run_slot=run_slot)
