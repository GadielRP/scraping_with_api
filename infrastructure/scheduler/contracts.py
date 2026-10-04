"""Small descriptions of scheduled work, without provider payloads or sessions."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable
from shared.execution_context import Priority


class Admission(str, Enum):
    ACCEPTED = "accepted"
    COALESCED = "coalesced"
    EXPIRED = "expired"
    FULL = "queue_full"
    CLOSED = "shutting_down"


@dataclass(frozen=True)
class JobRequest:
    key: str
    action: Callable[[], object] = field(repr=False, compare=False)
    scheduled_at: datetime
    priority: Priority
    expires_at: datetime | None = None
    budget_seconds: float | None = None
