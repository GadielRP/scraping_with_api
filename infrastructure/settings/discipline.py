"""Discipline-specific limits; shared transport and page limits remain in JobExecutionSettings."""
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DisciplineSettings:
    after_midnight_results_enabled: bool = True
    after_intraday_results_enabled: bool = True
    tracked_competitions_only: bool = True
    # Set only after verifying the provider's full-match, nonpaginated contract.
    full_feed_verified: bool = True
    max_events: int = 200
    budget_seconds: int = 900
    max_incidents: int = 1024
    max_players: int = 512
    max_relevant_ids: int = 8192

    def __post_init__(self):
        for name in ("max_events", "budget_seconds", "max_incidents",
                     "max_players", "max_relevant_ids"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
