"""Explicit deletion evidence, retaining set compatibility for old callers."""
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from shared.temporal import utc_now


@dataclass(frozen=True)
class DiscardEvidence:
    source: str
    source_event_id: str
    parser_kind: str
    reason: str
    snapshot: dict
    observed_at: datetime
    origin: str


class DeletionBatch(set):
    """IDs to delete plus evidence for classified deletions (404 has no kind).

    Passing this object intact to batch_delete_events preserves evidence.
    A bare set remains an administrative deletion, never an inferred canceled.
    """
    def __init__(self, iterable=(), *, origin='results'):
        super().__init__(iterable)
        self.evidence: dict[int, DiscardEvidence] = {}
        self.origin = origin

    def record(self, event_id, source_event_id, parsed, reason=None, *, source='sofascore'):
        self.add(event_id)
        self.evidence[event_id] = DiscardEvidence(
            source, str(source_event_id), parsed.kind, reason or parsed.kind,
            deepcopy(parsed.raw_snapshot), parsed.observed_at or utc_now(), self.origin,
        )
