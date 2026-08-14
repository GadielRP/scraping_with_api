"""SofaScore pre-start odds request and ingestion flow."""

from __future__ import annotations

import logging

from modules.jobs.pre_start_check_job.odds_source_state import (
    SOFASCORE_SOURCE,
    PreStartOddsSourceStates,
)
from modules.odds_ingestion import MarketOddsIngestionService, ProviderOddsSummary, run_provider_odds_phase
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

from .tennis_observations import enrich_tennis_observations

logger = logging.getLogger(__name__)


def run_sofascore_pre_start_odds(
    events_to_process: list[dict],
    source_states: PreStartOddsSourceStates,
    *,
    debug_mode: bool = False,
    odds_fetcher: SofaScoreOddsFetcher | None = None,
) -> ProviderOddsSummary:
    """Fetch eligible SofaScore odds and persist confirmed 404s in one update."""
    logger.info("🔵 SofaScore pre-start odds starting...")
    fetcher = odds_fetcher or SofaScoreOddsFetcher(api_client)

    def _has_resolved_sofascore_id(candidate: dict) -> bool:
        if candidate.get("sofascore_event_id") is None:
            logger.warning(
                "No sofascore_event_id available for event %s, skipping odds extraction",
                candidate["event_id"],
            )
            return False
        return True

    def _fetch_sofascore_odds(candidate: dict):
        sofascore_event_id = candidate["sofascore_event_id"]
        result = fetcher.fetch_odds(sofascore_event_id, candidate["event_data"].get("slug"))
        if result.endpoint_missing:
            logger.info(
                "🚫 SofaScore odds endpoint missing for event_id=%s sofascore_event_id=%s",
                candidate["event_id"],
                sofascore_event_id,
            )
        return result

    def _ingest_sofascore_odds(candidate: dict, payload: dict):
        event_data = candidate["event_data"]
        # Raw SofaScore JSON is passed through unchanged from odds_fetcher.
        # SofaScoreMarketAdapter + CanonicalMarketNormalizer stamp each choice
        # with mainLine=True and sourceMarketId=<catalog marketId> before
        # MarketRepository persists MarketChoiceQuote.source_market_id.
        return MarketOddsIngestionService.save_from_sofascore_response(
            candidate["event_id"],
            payload,
            source=SOFASCORE_SOURCE,
            home_team=event_data.get("home_team"),
            away_team=event_data.get("away_team"),
            debug_mode=debug_mode,
        )

    summary = run_provider_odds_phase(
        events_to_process,
        source_states,
        source=SOFASCORE_SOURCE,
        can_fetch=_has_resolved_sofascore_id,
        fetch=_fetch_sofascore_odds,
        ingest=_ingest_sofascore_odds,
        on_ingested=enrich_tennis_observations,
    )

    logger.info(
        "SofaScore pre-start odds summary: candidates=%s requests=%s ingested=%s "
        "skipped=%s failed=%s missing_endpoints=%s markets_saved=%s",
        summary.candidates_seen,
        summary.requests_attempted,
        summary.events_ingested,
        summary.events_skipped,
        summary.events_failed,
        summary.missing_endpoints,
        summary.markets_saved,
    )
    return summary
