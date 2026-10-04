"""SofaScore discovery source: filtered event iteration, no persistence."""

from collections import Counter
import logging
from modules.competition.discovery_scope import (
    source_competition_filter_reason,
    source_competition_ids,
)
from modules.sports.catalog import is_supported_sofascore_event
from modules.sofascore.streaming import bulk_document, document_entries

logger = logging.getLogger(__name__)


def iter_sport_events(client, date, sport, scope, store):
    counts = Counter()
    page = 1
    try:
        while True:
            controls = {}
            with bulk_document(
                client, f"/sport/{sport}/scheduled-tournaments/{date}/page/{page}"
            ) as document:
                for item in document_entries(document, "scheduled", controls=controls):
                    counts["tournaments_received"] += 1
                    reason = (
                        source_competition_filter_reason(item, scope) if scope is not None else None
                    )
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
                    with bulk_document(
                        client, f"/unique-tournament/{tournament_id}/scheduled-events/{date}"
                    ) as events:
                        for event in document_entries(events, "events"):
                            counts["events_received"] += 1
                            reason = (
                                source_competition_filter_reason(event, scope)
                                if scope is not None
                                else None
                            )
                            if reason or not is_supported_sofascore_event(event):
                                counts[reason or "unsupported_sport"] += 1
                                continue
                            yield event
            if not controls.get("hasNextPage", False):
                return
            page += 1
    finally:
        logger.info("Daily source sport=%s counts=%s", sport, dict(counts))
