"""Report generation for the regular-odds comparator.

Separated from the comparator (SRP): handles CSV, JSON, and text output.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from modules.oddspapi.diagnostics.regular_odds_comparator import (
    AggregateMetrics,
    ComparisonRow,
    ViabilityAssessment,
)


def generate_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

_CSV_COLUMNS = [
    "fixture_id", "sport_id", "minutes_until_start", "captured_at",
    "bookmaker", "source_market_id", "source_outcome_id", "player_id",
    "canonical_market_key", "canonical_choice_name", "handicap", "main_line",
    # /odds state
    "odds_present", "odds_market_active", "odds_player_active",
    "odds_price", "odds_limit", "odds_changed_at",
    # Historical state
    "historical_present", "historical_entry_count",
    "historical_active_entry_count", "historical_latest_active",
    "historical_latest_price", "historical_latest_limit",
    "historical_latest_created_at", "historical_latest_active_price",
    "historical_latest_active_created_at",
    "historical_opening_price", "historical_opening_created_at",
    "historical_observed_span_minutes",
    # Comparison
    "price_delta_latest", "price_delta_latest_active",
    "price_delta_pct_latest", "price_delta_pct_latest_active",
    "latest_price_matches", "latest_active_price_matches",
    "active_state_matches", "timestamp_delta_seconds",
    "current_only", "historical_only",
    "latest_historical_is_inactive", "historical_latest_is_stale",
    "comparison_status", "diagnostic_reasons",
]


def _row_to_csv_dict(row: ComparisonRow) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for col in _CSV_COLUMNS:
        val = getattr(row, col, None)
        if isinstance(val, list):
            val = "; ".join(str(v) for v in val)
        d[col] = val
    return d


def write_comparison_csv(
    rows: list[ComparisonRow],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_row_to_csv_dict(row))
    return output_path


def format_comparison_csv_string(rows: list[ComparisonRow]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_COLUMNS)
    writer.writeheader()
    for row in rows:
        writer.writerow(_row_to_csv_dict(row))
    return buf.getvalue()


# ---------------------------------------------------------------------------
# JSON summary output
# ---------------------------------------------------------------------------

def _serializable(obj: Any) -> Any:
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if hasattr(obj, "__dataclass_fields__"):
        return asdict(obj)
    return obj


def write_comparison_summary_json(
    aggregate: AggregateMetrics,
    viability: ViabilityAssessment,
    run_metadata: dict[str, Any],
    output_path: Path,
    *,
    dimension_rollups: dict[str, dict[str, AggregateMetrics]] | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "run_metadata": run_metadata,
        "aggregate_metrics": asdict(aggregate),
        "viability_assessment": asdict(viability),
    }
    if dimension_rollups:
        summary["dimension_rollups"] = {
            dim_name: {
                key: asdict(agg) for key, agg in dim_values.items()
            }
            for dim_name, dim_values in dimension_rollups.items()
        }
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=_serializable)
    return output_path


# ---------------------------------------------------------------------------
# Human-readable text report
# ---------------------------------------------------------------------------

def format_comparison_text(
    aggregate: AggregateMetrics,
    viability: ViabilityAssessment,
    run_metadata: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("OddsPapi Endpoint Comparison Report")
    lines.append("=" * 80)

    # Metadata
    for key, value in run_metadata.items():
        lines.append(f"  {key}: {value}")
    lines.append("")

    # Coverage
    lines.append("--- Coverage ---")
    lines.append(f"  Current outcome count:       {aggregate.current_outcome_count}")
    lines.append(f"  Historical outcome count:    {aggregate.historical_outcome_count}")
    lines.append(f"  Matched identity count:      {aggregate.matched_identity_count}")
    lines.append(f"  Current coverage in Historical: {_fmt_pct(aggregate.current_coverage_in_historical)}")
    lines.append(f"  Historical coverage in Current: {_fmt_pct(aggregate.historical_coverage_in_current)}")
    lines.append("")

    # Price matching
    lines.append("--- Price Matching ---")
    lines.append(f"  Exact price match rate:       {_fmt_pct(aggregate.exact_price_match_rate)}")
    lines.append(f"  Tolerance price match rate:   {_fmt_pct(aggregate.tolerance_price_match_rate)}")
    lines.append(f"  Latest-active match rate:     {_fmt_pct(aggregate.latest_active_price_match_rate)}")
    lines.append(f"  Active state agreement rate:  {_fmt_pct(aggregate.active_state_agreement_rate)}")
    lines.append("")

    # Deltas
    lines.append("--- Price Deltas ---")
    lines.append(f"  Mean absolute delta:    {_fmt_num(aggregate.mean_absolute_price_delta)}")
    lines.append(f"  Median absolute delta:  {_fmt_num(aggregate.median_absolute_price_delta)}")
    lines.append(f"  P95 absolute delta:     {_fmt_num(aggregate.p95_absolute_price_delta)}")
    lines.append(f"  Maximum absolute delta: {_fmt_num(aggregate.maximum_absolute_price_delta)}")
    lines.append("")

    # Timestamps
    lines.append("--- Timestamp Deltas ---")
    lines.append(f"  Mean timestamp delta:   {_fmt_num(aggregate.mean_timestamp_delta, suffix='s')}")
    lines.append(f"  P95 timestamp delta:    {_fmt_num(aggregate.p95_timestamp_delta, suffix='s')}")
    lines.append("")

    # Edge cases
    lines.append("--- Edge Cases ---")
    lines.append(f"  Current-only outcomes:    {aggregate.current_only_count}")
    lines.append(f"  Historical-only outcomes: {aggregate.historical_only_count}")
    lines.append(f"  Latest-inactive entries:  {aggregate.latest_inactive_count}")
    lines.append(f"  Stale historical entries: {aggregate.stale_historical_count}")
    lines.append(f"  Unmapped markets:         {aggregate.unmapped_market_count}")
    lines.append(f"  Unmapped outcomes:        {aggregate.unmapped_outcome_count}")
    lines.append("")

    # Performance
    if aggregate.odds_response_duration is not None or aggregate.historical_response_duration is not None:
        lines.append("--- Performance ---")
        lines.append(f"  /odds response duration:       {_fmt_num(aggregate.odds_response_duration, suffix='s')}")
        lines.append(f"  Historical response duration:  {_fmt_num(aggregate.historical_response_duration, suffix='s')}")
        lines.append(f"  /odds payload size:            {_fmt_bytes(aggregate.odds_payload_size)}")
        lines.append(f"  Historical payload size:       {_fmt_bytes(aggregate.historical_payload_size)}")
        lines.append("")

    # Viability
    lines.append("--- Viability Assessment ---")
    verdict = "YES ✅" if viability.historical_only_candidate else "NO ❌"
    lines.append(f"  Historical-only candidate: {verdict}")
    if viability.blocking_reasons:
        lines.append("  Blocking reasons:")
        for reason in viability.blocking_reasons:
            lines.append(f"    ❌ {reason}")
    if viability.warnings:
        lines.append("  Warnings:")
        for warning in viability.warnings:
            lines.append(f"    ⚠️  {warning}")
    lines.append("=" * 80)
    return "\n".join(lines)


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def _fmt_num(value: float | None, suffix: str = "") -> str:
    if value is None:
        return "N/A"
    return f"{value:.4f}{suffix}"


def _fmt_bytes(value: int | None) -> str:
    if value is None:
        return "N/A"
    if value >= 1_048_576:
        return f"{value / 1_048_576:.1f} MB"
    if value >= 1024:
        return f"{value / 1024:.1f} KB"
    return f"{value} B"
