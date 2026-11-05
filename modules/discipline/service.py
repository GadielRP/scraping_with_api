"""Acquire outside transactions, then reconcile one event under injected exclusion."""
from shared.execution_context import check_execution_budget
from .contracts import EventExclusion, EventReport, IncidentProvider, IncidentStore, Outcome


class DisciplineService:
    def __init__(self, provider: IncidentProvider, store: IncidentStore, exclusion: EventExclusion):
        self.provider, self.store, self.exclusion = provider, store, exclusion

    def process(self, context, *, commit=False):
        check_execution_budget()
        if context.mapping_count != 1 or context.mapping_id is None:
            return EventReport(Outcome.MAPPING_CONFLICT)
        sid = context.source_event_id
        if not isinstance(sid, str) or not sid.isascii() or not sid.isdecimal() or int(sid) <= 0:
            return EventReport(Outcome.INVALID)
        with self.exclusion(context.event_id) as acquired:
            if not acquired:
                return EventReport(Outcome.DEFERRED)
            acquisition = self.provider.acquire(context)
            if acquisition.outcome != Outcome.READY:
                return EventReport(acquisition.outcome, acquisition)
            check_execution_budget()
            outcome = self.store.replace_acquisition(context, acquisition, dry_run=not commit)
            return EventReport(outcome, acquisition)
