"""SofaScore's comments schema is confined to this incremental adapter."""
import re
import logging
from collections import Counter
from dataclasses import replace

from ijson import JSONError

from infrastructure.network.json_document import document_entries, JsonDocumentLimitExceeded
from modules.competition.competition_profiles import get_competition_profile
from modules.sofascore.exceptions import SofaScoreNotFoundException
from ..contracts import Acquisition, DisciplineEndpoint, DisciplineFact, DisciplineTotalDraft, FactKind, Outcome, Side
from ..policies import DisciplineLimitExceeded, evaluate_facts
from infrastructure.settings.discipline import DisciplineSettings
from shared.execution_context import check_execution_budget, WorkDeferred
from .sofascore_statistics import SofaScoreDisciplineStatisticsProvider

logger = logging.getLogger(__name__)


def _text(value):
    return value.strip() or None if isinstance(value, str) else None


def _identifier(value):
    return str(value).strip() or None if isinstance(value, (str, int)) and not isinstance(value, bool) else None


def _period(value):
    if not isinstance(value, str):
        return 0
    match = re.fullmatch(r"([1-4])Q", value)
    if match:
        return int(match[1])
    match = re.fullmatch(r"(?:(\d+))?OT", value)
    return 4 + int(match[1] or 1) if match else 0


class SofaScoreDisciplineProvider:
    def __init__(self, client, settings=None):
        self.client = client
        self.settings = settings or DisciplineSettings()

    def acquire(self, context):
        if context.source != "sofascore":
            return Acquisition(Outcome.INVALID, reason="unsupported_source")
        try:
            comments = self._acquire_comments(context)
        except (WorkDeferred, MemoryError, RecursionError):
            raise
        except Exception as exc:
            logger.warning("Discipline comments acquisition failed event_id=%s error=%s",
                           context.event_id, type(exc).__name__)
            comments = Acquisition(Outcome.FAILED, reason="comments_acquisition_failed")
        if comments.outcome == Outcome.DEFERRED:
            return comments
        usable = comments.outcome == Outcome.READY and bool(comments.incidents)
        result = comments if usable else replace(comments, incidents=(), managed_types=frozenset())
        if usable:
            counts = Counter((incident.type, incident.side) for incident in comments.incidents)
            result = replace(result, totals=tuple(
                DisciplineTotalDraft(kind, side, counts[kind, side], DisciplineEndpoint.COMMENTS)
                for kind in sorted(comments.managed_types) for side in Side))
        elif result.outcome == Outcome.READY:
            result = replace(result, outcome=Outcome.UNAVAILABLE, reason="no_comment_incidents")
        if self.settings.statistics_for_all_events or not usable:
            check_execution_budget()
            statistics = SofaScoreDisciplineStatisticsProvider(self.client).acquire(context)
            if statistics.outcome == Outcome.DEFERRED:
                return replace(statistics, statistics_attempted=True, comments_count=comments.comments_count)
            totals = {(total.type, total.side): total for total in result.totals}
            for total in statistics.totals:
                totals[total.type, total.side] = total
            result = replace(result, totals=tuple(totals.values()), statistics_attempted=True,
                             statistics_reason=statistics.reason)
            if statistics.outcome == Outcome.READY:
                result = replace(result, outcome=Outcome.READY)
        return result

    def _acquire_comments(self, context):
        profile = get_competition_profile(context.competition_id)
        rules = profile.discipline if profile else None
        controls = dict.fromkeys(("hasNextPage", "hasNext", "nextPage", "nextCursor"))
        coverage = dict(count=0, start=False, end=False, started_period=0, ended_period=0)

        def facts(document):
            for item in document_entries(document, "comments", controls=controls):
                coverage["count"] += 1
                if not isinstance(item, dict):
                    raise ValueError("comment must be an object")
                kind = item.get("type")
                if context.sport == "Football":
                    coverage["start"] |= kind == "matchStarted"
                    coverage["end"] |= kind == "matchEnded"
                    fact_kind = {"yellowCard": FactKind.YELLOW_CARD,
                                 "redCard": FactKind.RED_CARD}.get(kind)
                else:
                    period = _period(item.get("periodName"))
                    if kind == "periodStart":
                        coverage["start"] |= period == 1
                        coverage["started_period"] = max(coverage["started_period"], period)
                    elif kind == "periodEnd":
                        coverage["ended_period"] = max(coverage["ended_period"], period)
                    fact_kind = FactKind.EJECTION if kind == "ejection" else None
                    if kind == "foul" and rules is not None:
                        fact_kind = self._foul_kind(item)
                    # Eligibility exceptions may be described outside a foul entry.
                    text = (_text(item.get("text")) or "").lower()
                    if rules is not None and any(term in text for term in (
                        "no eligible", "no available substitutes", "all substitutes",
                        "disqualified player", "fewer than five", "less than five",
                    )):
                        yield DisciplineFact(FactKind.UNSUPPORTED_EXCEPTION, None, None, None)
                if fact_kind is None:
                    continue
                player = item.get("player") or {}
                if not isinstance(player, dict):
                    raise ValueError("player must be an object")
                side = Side.HOME if item.get("isHome") is True else Side.AWAY if item.get("isHome") is False else None
                external_id = _identifier(item.get("id"))
                sequence = _identifier(item.get("sequence"))
                identity = (("id", external_id) if external_id is not None else
                            ("sequence", sequence) if sequence is not None else None)
                yield DisciplineFact(fact_kind, _identifier(player.get("id")),
                                     _text(player.get("name")), side, identity)

        try:
            with self.client.open_event_comments(context.source_event_id) as document:
                result = evaluate_facts(context.sport, facts(document), rules, self.settings)
            result = replace(result, comments_count=coverage["count"])
            if result.outcome != Outcome.READY:
                return result
            if not coverage["count"]:
                return Acquisition(Outcome.UNAVAILABLE, reason="empty_comments",
                                   comments_count=coverage["count"])
            if any(value not in (None, False, "", 0) for value in controls.values()):
                return Acquisition(Outcome.UNAVAILABLE, reason="pagination_not_exhausted",
                                   comments_count=coverage["count"])
            complete = (coverage["start"] and coverage["end"] if context.sport == "Football" else
                        coverage["start"] and coverage["ended_period"] >= 4 and
                        coverage["ended_period"] >= coverage["started_period"])
            if not complete:
                return Acquisition(Outcome.UNAVAILABLE, reason="missing_match_boundaries",
                                   comments_count=coverage["count"])
            return result
        except SofaScoreNotFoundException:
            return Acquisition(Outcome.UNAVAILABLE, reason="comments_not_found")
        except (DisciplineLimitExceeded, JsonDocumentLimitExceeded) as exc:
            return Acquisition(Outcome.DEFERRED, reason=f"limit:{exc}",
                               comments_count=coverage["count"])
        except (ValueError, JSONError):
            return Acquisition(Outcome.INVALID, reason="invalid_comments_document",
                               comments_count=coverage["count"])

    @staticmethod
    def _foul_kind(item):
        # The sample has no structured foul subtype. Classify only the suffix
        # immediately following the reported player's name, never the drawn-by text.
        text = _text(item.get("text")) or ""
        player = item.get("player") or {}
        name = _text(player.get("name")) if isinstance(player, dict) else None
        suffix = text[len(name):].strip().lower() if name and text.startswith(name) else text.lower()
        if suffix.startswith("technical foul") or (not name and " technical foul" in suffix):
            return FactKind.TECHNICAL_FOUL
        if name and suffix.startswith(("personal foul", "shooting foul", "offensive foul")):
            return FactKind.PERSONAL_FOUL
        return FactKind.UNKNOWN_FOUL
