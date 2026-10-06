"""SofaScore discovery source: filtered event iteration, no persistence."""

from collections import Counter
import logging
from modules.jobs.discovery.filters import (
    sofascore_event_filter_reason,
    sofascore_tournament_filter_reason,
    source_competition_ids,
)
from infrastructure.network.json_document import document_entries

logger = logging.getLogger(__name__)


def iter_sport_events(client, date, sport, scope, store):
    counts = Counter()
    page = 1
    try:
        while True:
            controls = {"hasNextPage": False}
            with client.open_scheduled_tournaments(date, sport, page) as document:
                for item in document_entries(document, "scheduled", controls=controls):
                    counts["tournaments_received"] += 1
                    reason = sofascore_tournament_filter_reason(item, scope, sport)
                    if reason:
                        counts[reason] += 1
                        continue
                    if (
                        item.get("timezoneEventCount")
                        and sum(item["timezoneEventCount"].values()) == 0
                    ):
                        continue
                    tournament_id = source_competition_ids(item).source_unique_tournament_id
                    if tournament_id is None or not store.first_tournament(sport, tournament_id):
                        continue
                    with client.open_scheduled_events(tournament_id, date) as events:
                        for event in document_entries(events, "events"):
                            counts["events_received"] += 1
                            reason = sofascore_event_filter_reason(event, scope)
                            if reason:
                                counts[reason] += 1
                                continue
                            yield event
            if not controls.get("hasNextPage", False):
                return
            page += 1
    finally:
        logger.info("Daily source sport=%s counts=%s", sport, dict(counts))
