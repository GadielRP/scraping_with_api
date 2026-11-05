"""Bounded post-commit handoff shared by midnight and intraday results."""
import logging

from infrastructure.settings.discipline import DisciplineSettings
from modules.discipline.contracts import Selection
from shared.execution_context import WorkDeferred

logger = logging.getLogger(__name__)


def build_post_results_collector(*, origin="midnight", enabled=None, on_event=None):
    settings = DisciplineSettings()
    if enabled is None:
        enabled = {"midnight": settings.after_midnight_results_enabled,
                   "intraday": settings.after_intraday_results_enabled}[origin]
    if not enabled:
        return None
    if not settings.full_feed_verified:
        logger.warning("%s discipline disabled: full_feed_verified is False", origin)
        return None
    from .composition import build_discipline_runner

    run = build_discipline_runner(settings)

    def collect(events):
        ids = tuple(event.event_id for event in events)
        if not ids:
            return
        try:
            summary = run(
                selection=Selection(event_ids=ids),
                limit=len(ids),
                commit=True,
                on_event=on_event,
            )
            logger.info("%s discipline confirmed=%s selected=%s summary=%s", origin, len(events), len(ids), summary)
        except WorkDeferred:
            raise
        except Exception:
            # Result transactions have already committed; continue subsequent result pages.
            logger.exception("%s discipline batch failed selected=%s", origin, len(ids))

    return collect
