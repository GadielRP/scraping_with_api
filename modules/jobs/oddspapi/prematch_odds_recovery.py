"""Recover Oddspapi pre-match odds for fixtures with a recorded result."""

from __future__ import annotations

import logging
from datetime import timedelta
from itertools import groupby

from sqlalchemy import and_, or_
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import Event, EventSourceMapping, OddspapiMainlineOutcomeCache, Result
from infrastructure.persistence.repositories import MarketMappingRepository
from infrastructure.runtime.resource_budget import check_maintenance_capacity
from infrastructure.settings import Config
from modules.competition.tracked_competitions import tracked_competition_ids
from modules.jobs.pre_start_check_job.providers.oddspapi.constants import RESTRICTED_CAPTURE_MINUTE
from modules.jobs.pre_start_check_job.providers.oddspapi.event_selector import OddspapiPreStartCandidate
from modules.jobs.pre_start_check_job.providers.oddspapi.odds_batch_processor import OddspapiPreStartOddsBatchProcessor
from modules.jobs.pre_start_check_job.providers.oddspapi.settings import ODDSPAPI_PRE_START_SETTINGS
from shared.execution_context import WorkDeferred, check_execution_budget
from shared.temporal import local_day_bounds_utc

logger = logging.getLogger(__name__)


def recover_prematch_odds_for_finished_fixtures(target_date, critical_pending=None):
    recovery_start = Config.ODDSPAPI_HISTORY_RECOVERY_FROM_UTC
    if recovery_start is None:
        raise ValueError("Set ODDSPAPI_HISTORY_RECOVERY_FROM_UTC before recovering OddsPapi odds")
    day_start, day_end = local_day_bounds_utc(target_date, Config.TIMEZONE)
    mapping, cache = EventSourceMapping, OddspapiMainlineOutcomeCache
    with db_manager.get_session() as session:
        rows = (
            session.query(Event.id, mapping.source_event_id, Event.starts_at, mapping.source_sport_id, cache.bookmaker_slug)
            .select_from(Event)
            .join(Result, Result.event_id == Event.id)
            .join(mapping, and_(mapping.event_id == Event.id, mapping.source == "oddspapi"))
            .outerjoin(cache, and_(cache.event_id == Event.id, cache.fixture_id == mapping.source_event_id,
                                  cache.captured_at < Event.starts_at))
            .filter(Event.starts_at >= recovery_start, Event.starts_at < day_end,
                    or_(Event.starts_at >= day_start,
                        and_(Result.finished_at >= day_start, Result.finished_at < day_end)),
                    Event.competition_id.in_(tracked_competition_ids()))
            .distinct().order_by(Event.id, mapping.source_event_id).all()
        )
    if not rows:
        return
    regular = Config.ODDSPAPI_PRE_START_BOOKMAKERS or Config.ODDSPAPI_DEFAULT_BOOKMAKERS
    exchange = (Config.ODDSPAPI_PRE_START_EXCHANGE_BOOKMAKERS
                if Config.ENABLE_ODDSPAPI_EXCHANGE_HISTORICAL_REQUESTS else ())
    policy = ODDSPAPI_PRE_START_SETTINGS
    mappings = MarketMappingRepository.build_index(source="oddspapi", enabled_only=True)
    processor = OddspapiPreStartOddsBatchProcessor()
    remaining = policy.exchange_max_requests_per_run or None
    for (event_id, fixture_id, kickoff, sport), group in groupby(rows, key=lambda row: row[:4]):
        check_execution_budget()
        check_maintenance_capacity()
        if critical_pending is not None and critical_pending():
            raise WorkDeferred("OddsPapi history recovery is yielding to pre-start")
        cached = {row.bookmaker_slug for row in group}
        books = [slug for slug in regular or () if slug.lower() in cached]
        exchanges = [slug for slug in exchange or () if slug.lower() in cached]
        if not books and not exchanges:
            logger.info("OddsPapi history skipped fixture=%s reason=no_cache", fixture_id)
            continue
        summary = processor.process(
            [OddspapiPreStartCandidate(event_id, fixture_id, 0, source_sport_id=sport, starts_at=kickoff)],
            bookmakers=books, exchange_bookmakers=exchanges, market_mapping_index=mappings,
            exchange_market_keys=policy.exchange_market_keys,
            exchange_max_outcomes_per_event=policy.exchange_max_outcomes_per_event,
            exchange_max_requests_per_run=remaining or 0,
            enable_exchange_historical=remaining != 0,
            persist_main_line_only=True, require_active_quotes=Config.ODDSPAPI_PRE_START_REQUIRE_ACTIVE_QUOTES,
            available_through_utc=kickoff - timedelta(minutes=RESTRICTED_CAPTURE_MINUTE), recovery=True,
        )
        if remaining is not None:
            remaining -= summary.exchange_historical_requests_attempted
        if any(result.skip_reason == "oddspapi_quota_exhausted" for result in summary.results):
            return
