"""SofaScore pre-start odds request and ingestion flow."""

from __future__ import annotations

from collections.abc import Collection
import inspect
import logging


from modules.competition.tracked_competitions import (
    tracked_competition_ids as get_tracked_competition_ids,
)
from modules.jobs.pre_start_check_job.odds_source_state import (
    SOFASCORE_SOURCE,
    PreStartOddsSourceStates,
)
from modules.odds_ingestion import (
    MarketOddsIngestionService,
    ProviderOddsSummary,
    restrict_candidates_to_tracked_competitions,
    run_provider_odds_phase,
)
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

from .debug_response_writer import SofaScoreDebugResponseWriter
from .tennis_observations import enrich_tennis_observations

logger = logging.getLogger(__name__)


def _merge_summaries(*summaries: ProviderOddsSummary) -> ProviderOddsSummary:
    merged = ProviderOddsSummary()
    for s in summaries:
        merged.candidates_seen += s.candidates_seen
        merged.requests_attempted += s.requests_attempted
        merged.events_ingested += s.events_ingested
        merged.events_skipped += s.events_skipped
        merged.events_failed += s.events_failed
        merged.missing_endpoints += s.missing_endpoints
        merged.markets_saved += s.markets_saved
    return merged


def run_sofascore_pre_start_odds(
    events_to_process: list[dict],
    source_states: PreStartOddsSourceStates,
    *,
    debug_mode: bool = False,
    odds_fetcher: SofaScoreOddsFetcher | None = None,
    tracked_competition_ids: Collection[int] | None = None,
) -> ProviderOddsSummary:
    """Fetch eligible SofaScore odds and persist confirmed 404s in one update."""
    logger.info("🔵 SofaScore pre-start odds starting...")
    events_to_process = restrict_candidates_to_tracked_competitions(
        events_to_process,
        tracked_competition_ids,
        provider="SofaScore",
    )
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
        fetch_odds = fetcher.fetch_odds
        fetch_parameters = inspect.signature(fetch_odds).parameters
        supports_raw_capture = (
            "capture_raw_response" in fetch_parameters
            or any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD
                for parameter in fetch_parameters.values()
            )
        )
        fetch_kwargs = (
            {"capture_raw_response": debug_mode}
            if supports_raw_capture
            else {}
        )
        result = fetch_odds(
            sofascore_event_id,
            candidate["event_data"].get("slug"),
            **fetch_kwargs,
        )
        raw_payload = getattr(result, "raw_payload", None)
        if debug_mode and raw_payload is not None:
            SofaScoreDebugResponseWriter.save(
                event_id=candidate["event_id"],
                source_event_id=sofascore_event_id,
                minutes_until_start=candidate.get("minutes_until_start"),
                payload=raw_payload,
            )
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

    tracked_set = (
        set(tracked_competition_ids)
        if tracked_competition_ids is not None
        else set(get_tracked_competition_ids())
    )
    tracked_events = [
        e for e in events_to_process
        if (e.get("competition_id") or (e.get("event_data") or {}).get("competition_id")) in tracked_set
    ]
    untracked_events = [
        e for e in events_to_process
        if (e.get("competition_id") or (e.get("event_data") or {}).get("competition_id")) not in tracked_set
    ]

    summaries: list[ProviderOddsSummary] = []
    if tracked_events:
        logger.info(
            "🔵 [TRACKED] START SofaScore odds extraction (%s candidates)",
            len(tracked_events),
        )
        s_tracked = run_provider_odds_phase(
            tracked_events,
            source_states,
            source=SOFASCORE_SOURCE,
            can_fetch=_has_resolved_sofascore_id,
            fetch=_fetch_sofascore_odds,
            ingest=_ingest_sofascore_odds,
            on_ingested=enrich_tennis_observations,
        )
        logger.info(
            "🔵 [TRACKED] END SofaScore odds extraction (ingested=%s skipped=%s failed=%s)",
            s_tracked.events_ingested,
            s_tracked.events_skipped,
            s_tracked.events_failed,
        )
        summaries.append(s_tracked)

    if untracked_events:
        logger.info(
            "⚪ [UNTRACKED] START SofaScore odds extraction (%s candidates)",
            len(untracked_events),
        )
        s_untracked = run_provider_odds_phase(
            untracked_events,
            source_states,
            source=SOFASCORE_SOURCE,
            can_fetch=_has_resolved_sofascore_id,
            fetch=_fetch_sofascore_odds,
            ingest=_ingest_sofascore_odds,
            on_ingested=enrich_tennis_observations,
        )
        logger.info(
            "⚪ [UNTRACKED] END SofaScore odds extraction (ingested=%s skipped=%s failed=%s)",
            s_untracked.events_ingested,
            s_untracked.events_skipped,
            s_untracked.events_failed,
        )
        summaries.append(s_untracked)

    summary = _merge_summaries(*summaries) if summaries else ProviderOddsSummary()

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
