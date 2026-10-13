"""Stream admitted SofaScore events with resolved rounds, without persistence."""

import logging
from modules.jobs.discovery.filters import (
    admit_sofascore_event,
    sofascore_tournament_filter_reason,
    source_competition_ids,
)
from infrastructure.network.json_document import document_entries
from modules.sofascore.exceptions import SofaScoreNotFoundException
from .observability import DailySourceCounts

logger = logging.getLogger(__name__)


def iter_sport_events(client, date, sport, scope, store, counts: DailySourceCounts):
    page = 1
    try:
        while True:
            controls = {"hasNextPage": False}
            with client.open_scheduled_tournaments(date, sport, page) as document:
                for item in document_entries(document, "scheduled", controls=controls):
                    counts.totals["tournaments_received"] += 1
                    reason = sofascore_tournament_filter_reason(item, scope, sport)
                    if reason:
                        counts.tournament_filter_reasons[reason] += 1
                        continue
                    if (
                        item.get("timezoneEventCount")
                        and sum(item["timezoneEventCount"].values()) == 0
                    ):
                        counts.tournament_skip_reasons["empty_tournament_schedule"] += 1
                        continue
                    tournament_id = source_competition_ids(item).source_unique_tournament_id
                    if tournament_id is None:
                        counts.tournament_skip_reasons["missing_source_unique_tournament_id"] += 1
                        continue
                    if not store.first_tournament(sport, tournament_id):
                        counts.tournament_skip_reasons["duplicate_tournament"] += 1
                        continue
                    try:
                        with client.open_scheduled_events(tournament_id, date) as events:
                            for event in document_entries(events, "events"):
                                counts.totals["events_received"] += 1
                                admission = admit_sofascore_event(event, scope)
                                if admission.rejection_reason:
                                    counts.event_filter_reasons[admission.rejection_reason] += 1
                                    continue
                                counts.totals["events_admitted"] += 1
                                yield admission
                    except SofaScoreNotFoundException:
                        counts.tournament_skip_reasons["tournament_schedule_not_found"] += 1
                        logger.info(
                            "Daily tournament skipped sport=%s tournament_id=%s requested_date=%s "
                            "reason=tournament_schedule_not_found status=404",
                            sport, tournament_id, date,
                        )
            if not controls.get("hasNextPage", False):
                return
            page += 1
    finally:
        logger.info("Daily source sport=%s counts=%s", sport, counts.log_fields())
