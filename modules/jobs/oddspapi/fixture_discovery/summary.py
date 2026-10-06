"""Counters and summaries for fixture discovery, with bounded batch metric inputs."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from modules.oddspapi.event_resolver import OddspapiEventResolution


@dataclass
class OddspapiFixtureBatchResult:
    fixtures_valid: int = 0
    fixtures_deduplicated: int = 0
    invalid_payloads: int = 0
    fixtures_skipped_untracked_competition: int = 0
    fixtures_skipped_policy: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    resolved_existing_oddspapi: int = 0
    resolved_external_sofascore: int = 0
    resolved_candidate_match: int = 0
    mappings_created: int = 0
    unresolved_no_candidates: int = 0
    needs_review: int = 0
    queue_rows_written: int = 0
    ambiguous_queued: int = 0
    layer3_scored: int = 0
    shortlist_fallback_count: int = 0
    shortlist_widened_count: int = 0
    pool_candidate_counts: list[int] = field(default_factory=list)
    fuzzy_candidate_counts: list[int] = field(default_factory=list)
    score_duration_ms_values: list[float] = field(default_factory=list)
    resolutions: list[OddspapiEventResolution] | None = None

    def merge_metrics_from(self, other: "OddspapiFixtureBatchResult") -> None:
        self.fixtures_skipped_policy += int(getattr(other, "fixtures_skipped_policy", 0) or 0)
        for reason, count in getattr(other, "rejected_by_reason", {}).items():
            self.rejected_by_reason[reason] = self.rejected_by_reason.get(reason, 0) + count
        self.fixtures_valid += int(getattr(other, "fixtures_valid", 0) or 0)
        self.fixtures_deduplicated += int(getattr(other, "fixtures_deduplicated", 0) or 0)
        self.invalid_payloads += int(getattr(other, "invalid_payloads", 0) or 0)
        self.fixtures_skipped_untracked_competition += int(
            getattr(other, "fixtures_skipped_untracked_competition", 0) or 0
        )
        self.resolved_existing_oddspapi += int(getattr(other, "resolved_existing_oddspapi", 0) or 0)
        self.resolved_external_sofascore += int(getattr(other, "resolved_external_sofascore", 0) or 0)
        self.resolved_candidate_match += int(getattr(other, "resolved_candidate_match", 0) or 0)
        self.mappings_created += int(getattr(other, "mappings_created", 0) or 0)
        self.unresolved_no_candidates += int(getattr(other, "unresolved_no_candidates", 0) or 0)
        self.needs_review += int(getattr(other, "needs_review", 0) or 0)
        self.queue_rows_written += int(getattr(other, "queue_rows_written", 0) or 0)
        self.ambiguous_queued += int(getattr(other, "ambiguous_queued", 0) or 0)
        self.layer3_scored += int(getattr(other, "layer3_scored", 0) or 0)
        self.shortlist_fallback_count += int(getattr(other, "shortlist_fallback_count", 0) or 0)
        self.shortlist_widened_count += int(getattr(other, "shortlist_widened_count", 0) or 0)
        self.pool_candidate_counts.extend(list(getattr(other, "pool_candidate_counts", []) or []))
        self.fuzzy_candidate_counts.extend(list(getattr(other, "fuzzy_candidate_counts", []) or []))
        self.score_duration_ms_values.extend(list(getattr(other, "score_duration_ms_values", []) or []))


@dataclass
class SportFixtureDiscoverySummary:
    sport_slug: str
    sport_id: int
    requested_from: str
    requested_to: str
    fixtures_fetched: int = 0
    fixtures_valid: int = 0
    fixtures_deduplicated: int = 0
    invalid_payloads: int = 0
    fixtures_skipped_untracked_competition: int = 0
    fixtures_skipped_policy: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    resolved_existing_oddspapi: int = 0
    resolved_external_sofascore: int = 0
    resolved_candidate_match: int = 0
    mappings_created: int = 0
    unresolved_no_candidates: int = 0
    needs_review: int = 0
    queue_rows_written: int = 0
    errors: int = 0
    duration_seconds: float = 0.0

    def add_batch(self, batch: OddspapiFixtureBatchResult) -> None:
        """Keep sport totals without retaining per-fixture metric samples."""
        for name in (
            "fixtures_valid", "fixtures_deduplicated", "invalid_payloads",
            "fixtures_skipped_untracked_competition", "fixtures_skipped_policy",
            "resolved_existing_oddspapi", "resolved_external_sofascore",
            "resolved_candidate_match", "mappings_created", "unresolved_no_candidates",
            "needs_review", "queue_rows_written",
        ):
            setattr(self, name, getattr(self, name) + getattr(batch, name))
        for reason, count in batch.rejected_by_reason.items():
            self.rejected_by_reason[reason] = self.rejected_by_reason.get(reason, 0) + count


@dataclass
class OddspapiFixtureDiscoverySummary:
    started_at: datetime
    finished_at: datetime | None
    dry_run: bool
    create_mappings: bool
    persist_queue: bool
    sports: list[SportFixtureDiscoverySummary] = field(default_factory=list)

    @property
    def total_fixtures_fetched(self) -> int:
        return sum(item.fixtures_fetched for item in self.sports)

    @property
    def total_fixtures_skipped_policy(self) -> int:
        return sum(item.fixtures_skipped_policy for item in self.sports)

    @property
    def total_mappings_created(self) -> int:
        return sum(item.mappings_created for item in self.sports)

    @property
    def total_resolved_existing_oddspapi(self) -> int:
        return sum(item.resolved_existing_oddspapi for item in self.sports)

    @property
    def total_resolved_external_sofascore(self) -> int:
        return sum(item.resolved_external_sofascore for item in self.sports)

    @property
    def total_resolved_candidate_match(self) -> int:
        return sum(item.resolved_candidate_match for item in self.sports)

    @property
    def total_needs_review(self) -> int:
        return sum(item.needs_review for item in self.sports)

    @property
    def total_unresolved_no_candidates(self) -> int:
        return sum(item.unresolved_no_candidates for item in self.sports)

    @property
    def total_fixtures_skipped_untracked_competition(self) -> int:
        return sum(item.fixtures_skipped_untracked_competition for item in self.sports)

    def to_dict(self) -> dict:
        result = asdict(self)
        result.update(
            fixtures_skipped_policy=self.total_fixtures_skipped_policy,
            total_fixtures_fetched=self.total_fixtures_fetched,
            total_mappings_created=self.total_mappings_created,
            resolved_existing_oddspapi=self.total_resolved_existing_oddspapi,
            resolved_external_sofascore=self.total_resolved_external_sofascore,
            resolved_candidate_match=self.total_resolved_candidate_match,
            needs_review=self.total_needs_review,
            unresolved_no_candidates=self.total_unresolved_no_candidates,
            fixtures_skipped_untracked_competition=(
                self.total_fixtures_skipped_untracked_competition
            ),
        )
        return result


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * pct
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return float(ordered[low] * (1.0 - weight) + ordered[high] * weight)


def format_batch_metrics(result: OddspapiFixtureBatchResult) -> str:
    """Compact metric line for sport/batch completion logs."""
    pool = result.pool_candidate_counts
    fuzzy = result.fuzzy_candidate_counts
    score_ms = result.score_duration_ms_values
    return (
        f"l3_scored={result.layer3_scored} "
        f"ambiguous_queued={result.ambiguous_queued} "
        f"shortlist_fallback={result.shortlist_fallback_count} "
        f"shortlist_widened={result.shortlist_widened_count} "
        f"pool_p50={_percentile(pool, 0.50)} pool_p95={_percentile(pool, 0.95)} pool_max={max(pool) if pool else None} "
        f"fuzzy_p50={_percentile(fuzzy, 0.50)} fuzzy_p95={_percentile(fuzzy, 0.95)} fuzzy_max={max(fuzzy) if fuzzy else None} "
        f"score_ms_p50={_percentile(score_ms, 0.50)} score_ms_p95={_percentile(score_ms, 0.95)} "
        f"score_ms_max={max(score_ms) if score_ms else None}"
    )
