"""Daily source and write counts, independent of payloads and database reads."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from shared.runtime_observability import get_rss_mb

if TYPE_CHECKING:
    from .persistence import DailyWriteSummary


@dataclass(slots=True)
class DailySourceCounts:
    totals: Counter[str] = field(default_factory=Counter)
    event_filter_reasons: Counter[str] = field(default_factory=Counter)
    tournament_filter_reasons: Counter[str] = field(default_factory=Counter)
    tournament_skip_reasons: Counter[str] = field(default_factory=Counter)

    def snapshot(self) -> DailySourceCounts:
        return DailySourceCounts(
            self.totals.copy(), self.event_filter_reasons.copy(),
            self.tournament_filter_reasons.copy(), self.tournament_skip_reasons.copy(),
        )

    def since(self, previous: DailySourceCounts) -> DailySourceCounts:
        return DailySourceCounts(
            self.totals - previous.totals,
            self.event_filter_reasons - previous.event_filter_reasons,
            self.tournament_filter_reasons - previous.tournament_filter_reasons,
            self.tournament_skip_reasons - previous.tournament_skip_reasons,
        )

    def add(self, other: DailySourceCounts) -> None:
        self.totals.update(other.totals)
        self.event_filter_reasons.update(other.event_filter_reasons)
        self.tournament_filter_reasons.update(other.tournament_filter_reasons)
        self.tournament_skip_reasons.update(other.tournament_skip_reasons)

    def log_fields(self) -> dict:
        return {
            "events_received": self.totals["events_received"],
            "events_admitted": self.totals["events_admitted"],
            "events_filtered_at_source": sum(self.event_filter_reasons.values()),
            "source_filter_reasons": dict(self.event_filter_reasons),
            "tournaments_received": self.totals["tournaments_received"],
            "tournaments_filtered": sum(self.tournament_filter_reasons.values()),
            "tournament_filter_reasons": dict(self.tournament_filter_reasons),
            "tournaments_skipped": sum(self.tournament_skip_reasons.values()),
            "tournament_skip_reasons": dict(self.tournament_skip_reasons),
        }


def log_daily_batch(
    logger, source: DailySourceCounts, written: DailyWriteSummary,
    *, date, run_slot, sport, batch_number, phase="persistence",
) -> None:
    counts = source.log_fields()
    counts.update(
        events_persisted=written.persisted,
        events_inserted=written.inserted,
        events_updated=written.updated,
        events_blocked_by_discard_memory=written.blocked_by_discard_memory,
        events_filtered_before_write=written.filtered,
        events_filtered=counts["events_filtered_at_source"] + written.filtered,
        event_filter_reasons=dict(source.event_filter_reasons + written.filter_reasons),
        write_filter_reasons=dict(written.filter_reasons),
        events_failed=written.failed,
        events_unprocessed=source.totals["events_admitted"] if phase == "incomplete" else 0,
    )
    logger.info(
        "Daily discovery batch requested_date=%s slot=%s sport=%s batch=%s "
        "process_rss_mb=%s phase=%s counts=%s",
        date, run_slot, sport, batch_number, get_rss_mb(), phase, counts,
    )
