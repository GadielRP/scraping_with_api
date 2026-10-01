"""Validated operational policy for confirmed event deletions."""
from dataclasses import dataclass


@dataclass(frozen=True)
class DiscardSettings:
    """Validated settings snapshot sourced from the application environment config."""

    enabled: bool
    kinds: frozenset[str]
    retention_days: int
    cleanup_enabled: bool
    cleanup_interval_hours: int
    batch_size: int
    cleanup_batch_size: int

    def __post_init__(self):
        if not self.kinds <= {'canceled', 'not_started', 'finished_empty_score'}:
            raise ValueError('Discard memory only accepts kinds eligible for deletion')
        for name in ('retention_days', 'cleanup_interval_hours', 'batch_size', 'cleanup_batch_size'):
            if getattr(self, name) <= 0:
                raise ValueError(f'{name} must be positive')

    @classmethod
    def current(cls):
        from infrastructure.settings import Config
        return cls(
            enabled=Config.EVENT_DISCARD_MEMORY_ENABLED,
            kinds=frozenset(Config.EVENT_DISCARD_MEMORY_KINDS),
            retention_days=Config.EVENT_DISCARD_MEMORY_RETENTION_DAYS,
            cleanup_enabled=Config.EVENT_DISCARD_MEMORY_CLEANUP_ENABLED,
            cleanup_interval_hours=Config.EVENT_DISCARD_MEMORY_CLEANUP_INTERVAL_HOURS,
            batch_size=Config.EVENT_WRITE_BATCH_SIZE,
            cleanup_batch_size=Config.EVENT_DISCARD_MEMORY_CLEANUP_BATCH_SIZE,
        )
