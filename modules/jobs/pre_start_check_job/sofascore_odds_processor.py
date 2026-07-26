"""SofaScore pre-start odds request and ingestion flow."""

from __future__ import annotations

from dataclasses import dataclass
import logging

from infrastructure.persistence.repositories import (
    EventSourceMappingRepository,
)
from modules.observations import sport_observation_service
from modules.odds_ingestion import MarketOddsIngestionService
from modules.sofascore import api_client
from modules.sofascore.odds_fetcher import SofaScoreOddsFetcher

from .odds_source_state import SOFASCORE_SOURCE, PreStartOddsSourceStates

logger = logging.getLogger(__name__)


@dataclass
class SofaScorePreStartOddsSummary:
    """Operational result of one SofaScore pre-start odds phase."""

    candidates_seen: int = 0
    requests_attempted: int = 0
    events_ingested: int = 0
    events_skipped: int = 0
    events_failed: int = 0
    missing_endpoints: int = 0
    markets_saved: int = 0


def process_sofascore_pre_start_odds(
    events_to_process: list[dict],
    source_states: PreStartOddsSourceStates,
    *,
    debug_mode: bool = False,
    odds_fetcher: SofaScoreOddsFetcher | None = None,
) -> SofaScorePreStartOddsSummary:
    """Fetch eligible SofaScore odds and persist confirmed 404s in one update."""
    summary = SofaScorePreStartOddsSummary(candidates_seen=len(events_to_process or []))
    odds_not_found_event_ids: set[int] = set()
    fetcher = odds_fetcher or SofaScoreOddsFetcher(api_client)

    for event_info in events_to_process:
        try:
            event_data = event_info["event_data"]
            event_id = event_data["id"]

            if not event_info["should_extract_odds"]:
                summary.events_skipped += 1
                continue

            source_state = source_states.get(event_id, {}).get(SOFASCORE_SOURCE)
            if source_state is not None and not source_state.has_odds:
                logger.info(
                    "🚫 Skipping SofaScore odds request for event_id=%s: endpoint marked unavailable",
                    event_id,
                )
                summary.events_skipped += 1
                continue

            sofascore_event_id = event_info.get("sofascore_event_id")
            if sofascore_event_id is None:
                logger.warning(
                    "No sofascore_event_id available for event %s, skipping odds extraction",
                    event_id,
                )
                summary.events_skipped += 1
                continue

            summary.requests_attempted += 1
            fetch_result = fetcher.fetch_odds(
                sofascore_event_id,
                event_data["slug"],
            )
            if fetch_result.endpoint_missing:
                odds_not_found_event_ids.add(event_id)
                summary.missing_endpoints += 1
                summary.events_skipped += 1
                logger.info(
                    "SofaScore odds endpoint missing for event_id=%s sofascore_event_id=%s",
                    event_id,
                    sofascore_event_id,
                )
                continue

            final_odds_response = fetch_result.payload
            if not final_odds_response:
                summary.events_skipped += 1
                continue

            event_info["odds_response"] = final_odds_response
            ingestion_result = MarketOddsIngestionService.save_from_event_odds_response(
                event_id,
                final_odds_response,
                source=SOFASCORE_SOURCE,
                home_team=event_data.get("home_team"),
                away_team=event_data.get("away_team"),
                debug_mode=debug_mode,
            )
            event_info["ingestion_result"] = ingestion_result
            summary.markets_saved += ingestion_result.markets_saved or 0
            if ingestion_result.markets_saved > 0 or ingestion_result.dual_process_market_available:
                summary.events_ingested += 1

                if event_data["sport"] in ["Tennis", "Tennis Doubles"]:
                    if not sport_observation_service.event_has_observations(event_id):
                        snapshot = event_info.get("metadata_snapshot")
                        if snapshot and snapshot.get("observations"):
                            event_info["observations"] = snapshot["observations"]
                        else:
                            observations = api_client.get_event_results(
                                sofascore_event_id,
                                canonical_event_id=event_id,
                                update_court_type=True,
                            )
                            if observations:
                                event_info["observations"] = observations
            else:
                summary.events_skipped += 1
                logger.warning(
                    "No market odds saved for event %s: %s",
                    event_id,
                    ingestion_result.reason,
                )
        except Exception as exc:
            summary.events_failed += 1
            logger.error(
                "Error processing upcoming event odds %s: %s",
                event_info.get("event_id", "unknown"),
                exc,
            )

    if odds_not_found_event_ids:
        EventSourceMappingRepository.mark_odds_unavailable(
            odds_not_found_event_ids,
            SOFASCORE_SOURCE,
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
