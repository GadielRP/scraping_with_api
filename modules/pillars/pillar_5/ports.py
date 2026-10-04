"""Interfaces between the memory application, SQL lookup and audit readers."""

from datetime import datetime
from typing import Protocol
from .calculation_models import MemoryQueryKey, MemorySample, PopulationFilters


class PriceMemoryReader(Protocol):
    def summarize(
        self,
        *,
        key: MemoryQueryKey,
        current_event_id: int,
        current_starts_at: datetime,
        population_filters: PopulationFilters
    ) -> MemorySample: ...


class SampleAuditReader(Protocol):
    def get_sample_page(
        self,
        sample_id: str,
        cursor: tuple[str, int] | None = None,
        page_size: int = 100,
    ) -> dict: ...
