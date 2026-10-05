"""Execution and memory limits. Configure these values here, then restart."""

from dataclasses import dataclass


@dataclass(frozen=True)
class JobExecutionSettings:
    maintenance_capacity: int = 8
    event_read_batch_size: int = 100
    event_write_batch_size: int = 100
    pre_start_budget_seconds: int = 240
    response_max_bytes: int = 128 * 1024 * 1024
    json_item_max_bytes: int = 2 * 1024 * 1024
    json_max_depth: int = 128
    temporary_cache_kib: int = 2048
    view_refresh_poll_seconds: int = 15 * 60
    view_refresh_retry_seconds: int = 60
    view_refresh_timeout_ms: int = 180000
    view_refresh_min_interval_seconds: int = 60 * 60
    missing_odds_cooldown_seconds: int = 600
    missing_odds_capacity: int = 2048
    db_pool_size: int = 6
    db_pool_timeout_seconds: int = 10
    maintenance_min_available_mb: int = 96
    shutdown_grace_seconds: int = 30

    def __post_init__(self):
        for name in self.__dataclass_fields__:
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
