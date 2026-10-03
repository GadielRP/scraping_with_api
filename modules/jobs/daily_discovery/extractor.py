"""Daily discovery extractor for SofaScore scheduled events."""

from __future__ import annotations

import logging
from collections import Counter
from typing import Dict, List

from infrastructure.persistence.repositories import DailyDiscoveryRepository
from modules.jobs.discovery_filters import filter_upcoming_events
from modules.jobs.discovery_persistence_summary import DiscoveryPersistenceSummary
from modules.sofascore import api_client as default_api_client
from modules.competition.discovery_scope import (
    load_source_competitions,
    load_tracked_source_competitions,
    source_competition_filter_reason,
    source_competition_ids,
)
from modules.sports.catalog import is_supported_sofascore_event, sofascore_sport_slugs
from shared.shutdown import is_shutdown_requested

from .odds_parser import parse_today_market_odds_response
from .persistence import persist_events_and_optional_odds

logger = logging.getLogger(__name__)


class DailyDiscoveryExtractor:
    """Extract and persist today's events with odds."""

    def __init__(self, api_client=None):
        self.api_client = api_client or default_api_client

    def discover_events_for_date(
        self,
        date: str,
        sports: List[str] | None = None,
        run_slot: str | None = None,
    ) -> Dict[str, int]:
        stats = dict.fromkeys(("events_processed", "events_persisted", "events_inserted",
                               "events_updated", "events_discarded", "events_failed", "odds_inserted"), 0)
        sports = sofascore_sport_slugs(sports)
        if not sports:
            return stats
        tracked_competitions = load_tracked_source_competitions("sofascore")
        tracked_competition_scope = (
            tracked_competitions
            if tracked_competitions is not None
            else load_source_competitions("sofascore")
        )
        tracked_filter_enabled = tracked_competitions is not None
        logger.info(
            "Daily discovery scope date=%s sports=%s tracked_competition_filter=%s "
            "provider_competition_pairs=%s",
            date,
            sports,
            "enabled" if tracked_filter_enabled else "disabled_observation_only",
            len(tracked_competition_scope),
        )
        if tracked_competitions is not None and not tracked_competitions:
            logger.warning(
                "Daily discovery stopped reason=tracked_competition_scope_empty; "
                "no SofaScore provider IDs are mapped for tracked competitions"
            )
            return stats
        if not tracked_filter_enabled:
            logger.warning(
                "Daily discovery tracked competition filter is disabled; "
                "all events remain eligible and logs will show shadow rejections "
                "reason=DISCOVERY_TRACKED_COMPETITIONS_ONLY_false"
            )

        normalized_run_slot = (run_slot or "AM").strip().upper()
        if normalized_run_slot not in {"AM", "PM"}:
            logger.warning(
                "Daily discovery extractor received invalid run_slot=%s; defaulting to AM.",
                run_slot,
            )
            normalized_run_slot = "AM"
        elif run_slot is None:
            logger.warning(
                "Daily discovery extractor invoked without run_slot; defaulting to AM for backward compatibility."
            )

        logger.info("Starting daily discovery for date: %s", date)
        persistence_summary = DiscoveryPersistenceSummary()

        try:
            for sport in sports:
                if is_shutdown_requested():
                    raise KeyboardInterrupt()

                try:
                    logger.info("Processing %s...", sport)
                    logger.info("Fetching today's %s scheduled tournaments...", sport)

                    unique_tournament_ids = []
                    tournament_filter_counts = Counter()
                    page = 1
                    failed = False

                    while True:
                        page_response = self.api_client.get_today_sport_events_response(date, sport, page)

                        if not page_response:
                            failed = True
                            break

                        scheduled = page_response.get("scheduled", [])
                        if not scheduled:
                            break

                        for item in scheduled:
                            tournament_filter_counts["response_entries"] += 1
                            tz_count = item.get("timezoneEventCount", {})
                            if tz_count:
                                if sum(tz_count.values()) == 0:
                                    tournament_filter_counts["rejected_zero_timezone_event_count"] += 1
                                    continue

                            ids = source_competition_ids(item)
                            tracked_reason = source_competition_filter_reason(item, tracked_competition_scope)
                            if tracked_reason:
                                prefix = "rejected" if tracked_filter_enabled else "would_reject"
                                tournament_filter_counts[f"{prefix}_{tracked_reason}"] += 1
                                if tracked_filter_enabled:
                                    continue

                            ut_id = ids.source_unique_tournament_id
                            if ut_id is None:
                                tournament_filter_counts["rejected_missing_unique_tournament_id"] += 1
                                continue
                            if ut_id in unique_tournament_ids:
                                tournament_filter_counts["duplicate_unique_tournament_id"] += 1
                            else:
                                unique_tournament_ids.append(ut_id)

                        if not page_response.get("hasNextPage", False):
                            break

                        page += 1

                    logger.info(
                        "Daily tournament filter sport=%s mode=%s provider_pairs=%s "
                        "received=%s rejected_untracked=%s rejected_missing_ids=%s "
                        "would_reject_untracked=%s would_reject_missing_ids=%s "
                        "rejected_zero_timezone_events=%s missing_unique_id=%s "
                        "duplicates=%s selected_for_event_fetch=%s",
                        sport,
                        "enforced" if tracked_filter_enabled else "observe_only",
                        len(tracked_competition_scope),
                        tournament_filter_counts["response_entries"],
                        tournament_filter_counts["rejected_untracked_competition"],
                        tournament_filter_counts["rejected_missing_source_competition_ids"],
                        tournament_filter_counts["would_reject_untracked_competition"],
                        tournament_filter_counts[
                            "would_reject_missing_source_competition_ids"
                        ],
                        tournament_filter_counts["rejected_zero_timezone_event_count"],
                        tournament_filter_counts["rejected_missing_unique_tournament_id"],
                        tournament_filter_counts["duplicate_unique_tournament_id"],
                        len(unique_tournament_ids),
                    )

                    if failed:
                        logger.warning("Incomplete tournaments response for %s, leaving slot retryable", sport)
                        DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "failed")
                        continue

                    logger.info("Found %d unique tournaments for %s. Fetching events...", len(unique_tournament_ids), sport)

                    all_events = []
                    event_filter_counts = Counter()
                    for ut_id in unique_tournament_ids:
                        try:
                            ut_events_response = self.api_client.get_unique_tournament_scheduled_events(ut_id, date)
                            if not ut_events_response or "events" not in ut_events_response:
                                failed = True
                                event_filter_counts["tournaments_without_events_payload"] += 1
                                logger.warning(
                                    "Daily discovery received no tournament events "
                                    "unique_tournament_id=%s reason=missing_events_payload",
                                    ut_id,
                                )
                                continue
                            tournament_events = ut_events_response.get("events") or []
                            for event in tournament_events:
                                event_filter_counts["response_events"] += 1
                                if not is_supported_sofascore_event(event):
                                    event_filter_counts["rejected_unsupported_sport"] += 1
                                    continue
                                tracked_reason = source_competition_filter_reason(event, tracked_competition_scope)
                                if tracked_reason:
                                    prefix = "rejected" if tracked_filter_enabled else "would_reject"
                                    event_filter_counts[f"{prefix}_{tracked_reason}"] += 1
                                    if tracked_filter_enabled:
                                        continue
                                all_events.append(event)
                        except Exception as exc:
                            failed = True
                            event_filter_counts["tournament_fetch_errors"] += 1
                            logger.warning(
                                "Failed to fetch events for tournament %s "
                                "reason=tournament_event_fetch_error error=%s",
                                ut_id,
                                exc,
                            )

                    logger.info(
                        "Daily event filter sport=%s mode=%s response_events=%s "
                        "rejected_unsupported_sport=%s rejected_untracked=%s "
                        "rejected_missing_ids=%s would_reject_untracked=%s "
                        "would_reject_missing_ids=%s eligible_events=%s "
                        "tournaments_without_events=%s fetch_errors=%s",
                        sport,
                        "enforced" if tracked_filter_enabled else "observe_only",
                        event_filter_counts["response_events"],
                        event_filter_counts["rejected_unsupported_sport"],
                        event_filter_counts["rejected_untracked_competition"],
                        event_filter_counts["rejected_missing_source_competition_ids"],
                        event_filter_counts["would_reject_untracked_competition"],
                        event_filter_counts[
                            "would_reject_missing_source_competition_ids"
                        ],
                        len(all_events),
                        event_filter_counts["tournaments_without_events_payload"],
                        event_filter_counts["tournament_fetch_errors"],
                    )

                    if not all_events:
                        logger.info(
                            "No daily discovery events remain sport=%s "
                            "reason=no_events_after_sport_and_tracked_competition_filters",
                            sport,
                        )
                        DailyDiscoveryRepository.update_sport_status(
                            date, normalized_run_slot, sport, "failed" if failed else "completed"
                        )
                        continue

                    logger.info("Fetching today's %s odds for tracked events...", sport)
                    odds_map = {}
                    try:
                        odds_response = self.api_client.get_today_sport_events_odds_response(date, sport)
                        if odds_response:
                            odds_map = parse_today_market_odds_response(
                                odds_response,
                                event_ids={int(event["id"]) for event in all_events if event.get("id")},
                            )
                        else:
                            logger.warning("No odds response for %s, proceeding without odds", sport)
                    except Exception as exc:
                        logger.warning("Failed to fetch odds for %s: %s. Will proceed without odds.", sport, exc)

                    # Determine which events have not started yet (using our standard min_minutes_away=10 threshold)
                    upcoming_events = filter_upcoming_events(all_events, min_minutes_away=10)
                    upcoming_event_ids = {e["id"] for e in upcoming_events if e.get("id")}

                    logger.info("Processing %s %s events...", len(all_events), sport)

                    selected_odds = {sid: odds for sid, odds in odds_map.items() if sid in upcoming_event_ids}
                    write_summary = persist_events_and_optional_odds(
                        self.api_client, all_events, selected_odds,
                        tracked_competitions=tracked_competitions,
                        persistence_summary=persistence_summary,
                    )
                    stats["events_processed"] += len(all_events)
                    stats["events_persisted"] += write_summary.persisted
                    stats["events_inserted"] += write_summary.inserted
                    stats["events_updated"] += write_summary.updated
                    stats["events_discarded"] += write_summary.discarded
                    stats["events_failed"] += write_summary.failed
                    stats["odds_inserted"] += write_summary.odds_saved

                    failed = failed or write_summary.failed > 0
                    logger.info("Daily persistence sport=%s persisted=%s inserted=%s updated=%s discarded=%s out_of_scope=%s failed=%s",
                                sport, write_summary.persisted, write_summary.inserted, write_summary.updated, write_summary.discarded,
                                write_summary.out_of_scope, write_summary.failed)

                    DailyDiscoveryRepository.update_sport_status(
                        date, normalized_run_slot, sport, "failed" if failed else "completed"
                    )
                    logger.info(
                        "%s status=%s: %s/%s events persisted, %s with odds",
                        sport,
                        "failed" if failed else "completed",
                        write_summary.persisted,
                        len(all_events),
                        write_summary.odds_saved,
                    )

                except Exception as exc:
                    logger.error("Error processing %s: %s", sport, exc)
                    DailyDiscoveryRepository.update_sport_status(date, normalized_run_slot, sport, "failed")
                    continue

            logger.info("Daily discovery persistence summary: %s", stats)
            return stats
        except Exception as exc:
            logger.error("Error in discover_events_for_date: %s", exc)
            return stats
        finally:
            persistence_summary.log(
                logger, job="daily_discovery", requested_date=date, run_slot=normalized_run_slot,
            )
