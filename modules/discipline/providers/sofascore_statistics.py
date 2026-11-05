"""Bounded football discipline aggregates from the full-match statistics period."""
import logging

from ijson import JSONError

from infrastructure.network.json_document import document_entries, JsonDocumentLimitExceeded
from modules.sofascore.exceptions import SofaScoreNotFoundException
from shared.execution_context import WorkDeferred
from ..contracts import Acquisition, DisciplineEndpoint, DisciplineTotalDraft, IncidentType, Outcome, Side

logger = logging.getLogger(__name__)


class SofaScoreDisciplineStatisticsProvider:
    def __init__(self, client):
        self.client = client

    def acquire(self, context):
        if context.source != "sofascore":
            return Acquisition(Outcome.UNAVAILABLE, reason="unsupported_statistics_source")
        values = {}
        keys = {"yellowCards": IncidentType.YELLOW_CARD, "redCards": IncidentType.RED_CARD}
        try:
            with self.client.open_event_statistics(context.source_event_id) as document:
                for period in document_entries(document, "statistics"):
                    if not isinstance(period, dict):
                        raise ValueError("Statistics period must be an object")
                    if period.get("period") != "ALL":
                        continue
                    for group in period.get("groups", []):
                        for item in group.get("statisticsItems", []):
                            kind = keys.get(item.get("key"))
                            if kind is None:
                                continue
                            pair = (item.get("homeValue"), item.get("awayValue"))
                            if any(isinstance(v, bool) or not isinstance(v, int) or v < 0 for v in pair):
                                continue
                            if kind in values and values[kind] != pair:
                                raise ValueError("Conflicting full-match discipline statistics")
                            values[kind] = pair
            totals = tuple(DisciplineTotalDraft(kind, side, count, DisciplineEndpoint.STATISTICS)
                           for kind, pair in values.items() for side, count in zip(Side, pair))
            return Acquisition(Outcome.READY, totals=totals) if totals else Acquisition(
                Outcome.UNAVAILABLE, reason="no_discipline_incidents_statistics")
        except (WorkDeferred, MemoryError, RecursionError):
            raise
        except JsonDocumentLimitExceeded:
            return Acquisition(Outcome.DEFERRED, reason="statistics_resource_limit")
        except SofaScoreNotFoundException:
            return Acquisition(Outcome.UNAVAILABLE, reason="statistics_not_found")
        except (ValueError, JSONError, TypeError, AttributeError):
            return Acquisition(Outcome.INVALID, reason="invalid_statistics_document")
        except Exception as exc:
            logger.warning("Discipline statistics acquisition failed event_id=%s error=%s",
                           context.event_id, type(exc).__name__)
            return Acquisition(Outcome.FAILED, reason="statistics_acquisition_failed")
