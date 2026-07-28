"""Pure comparator: /v4/odds vs /v4/historical-odds for regular bookmakers.

This module has **no** HTTP, database, or Config dependencies.
All configuration is received as explicit parameters.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Exchange bookmaker guard
# ---------------------------------------------------------------------------

EXCHANGE_BOOKMAKER_SLUGS: frozenset[str] = frozenset({"betfair-ex"})


def is_exchange_bookmaker(slug: str) -> bool:
    return str(slug).strip().lower() in EXCHANGE_BOOKMAKER_SLUGS


def reject_exchange_bookmakers(bookmakers: Sequence[str]) -> list[str]:
    """Return non-exchange bookmakers; raise if any exchange slug is found."""
    rejected = [b for b in bookmakers if is_exchange_bookmaker(b)]
    if rejected:
        raise ValueError(
            f"Exchange bookmakers are not supported by this comparator: "
            f"{', '.join(rejected)}"
        )
    return list(bookmakers)


# ---------------------------------------------------------------------------
# Comparison status
# ---------------------------------------------------------------------------

class ComparisonStatus(str, Enum):
    MATCH = "MATCH"
    PRICE_MISMATCH = "PRICE_MISMATCH"
    ACTIVE_STATE_MISMATCH = "ACTIVE_STATE_MISMATCH"
    HISTORICAL_LATEST_INACTIVE = "HISTORICAL_LATEST_INACTIVE"
    HISTORICAL_STALE = "HISTORICAL_STALE"
    CURRENT_ONLY = "CURRENT_ONLY"
    HISTORICAL_ONLY = "HISTORICAL_ONLY"
    INVALID_CURRENT_PRICE = "INVALID_CURRENT_PRICE"
    INVALID_HISTORICAL_PRICE = "INVALID_HISTORICAL_PRICE"
    INVALID_HISTORICAL_TIMESTAMP = "INVALID_HISTORICAL_TIMESTAMP"
    UNMAPPED_MARKET = "UNMAPPED_MARKET"
    UNMAPPED_OUTCOME = "UNMAPPED_OUTCOME"


# ---------------------------------------------------------------------------
# Configuration (no Config coupling)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ComparisonConfig:
    price_tolerance: float = 0.001
    stale_threshold_seconds: float = 60.0
    minimum_opening_span_minutes: float = 60.0


# ---------------------------------------------------------------------------
# Identity key
# ---------------------------------------------------------------------------

def _identity_key(
    bookmaker: str,
    source_market_id: str,
    source_outcome_id: str,
    player_id: str,
) -> tuple[str, str, str, str]:
    return (
        str(bookmaker).strip().lower(),
        str(source_market_id).strip(),
        str(source_outcome_id).strip(),
        str(player_id).strip(),
    )


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return None
        return result
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Parsed entries from /v4/odds
# ---------------------------------------------------------------------------

@dataclass
class OddsEntry:
    fixture_id: str | None
    sport_id: str | None
    bookmaker: str
    source_market_id: str
    source_outcome_id: str
    player_id: str
    market_active: bool | None
    player_active: bool | None
    price: float | None
    limit: float | None
    changed_at: str | None
    bookmaker_changed_at: str | None
    main_line: bool | None
    bookmaker_outcome_id: str | None

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return _identity_key(
            self.bookmaker,
            self.source_market_id,
            self.source_outcome_id,
            self.player_id,
        )


# ---------------------------------------------------------------------------
# Parsed/summarised entries from /v4/historical-odds
# ---------------------------------------------------------------------------

@dataclass
class HistoricalEntry:
    """One raw entry from the historical timeline."""
    created_at: datetime | None
    created_at_raw: str | None
    price: float | None
    limit: float | None
    active: bool | None
    timestamp_valid: bool = True


@dataclass
class HistoricalSummary:
    fixture_id: str | None
    bookmaker: str
    source_market_id: str
    source_outcome_id: str
    player_id: str
    total_entry_count: int = 0
    active_entry_count: int = 0
    entries: list[HistoricalEntry] = field(default_factory=list)
    # Derived from sorted valid entries
    earliest_active_entry: HistoricalEntry | None = None
    latest_entry: HistoricalEntry | None = None
    latest_active_entry: HistoricalEntry | None = None
    invalid_timestamp_count: int = 0

    @property
    def identity(self) -> tuple[str, str, str, str]:
        return _identity_key(
            self.bookmaker,
            self.source_market_id,
            self.source_outcome_id,
            self.player_id,
        )


# ---------------------------------------------------------------------------
# Enrichment info (optional canonical mapping)
# ---------------------------------------------------------------------------

@dataclass
class EnrichmentInfo:
    source_sport_id: str | None = None
    canonical_market_key: str | None = None
    canonical_market_name: str | None = None
    canonical_market_group: str | None = None
    canonical_market_period: str | None = None
    market_family: str | None = None
    canonical_choice_name: str | None = None
    handicap: str | None = None
    choice_group: str | None = None


# ---------------------------------------------------------------------------
# Comparison row (one per identity)
# ---------------------------------------------------------------------------

@dataclass
class ComparisonRow:
    # Identity
    fixture_id: str | None = None
    sport_id: str | None = None
    minutes_until_start: float | None = None
    captured_at: str | None = None
    bookmaker: str | None = None
    source_market_id: str | None = None
    source_outcome_id: str | None = None
    player_id: str | None = None
    canonical_market_key: str | None = None
    canonical_choice_name: str | None = None
    handicap: str | None = None
    main_line: bool | None = None

    # /odds state
    odds_present: bool = False
    odds_market_active: bool | None = None
    odds_player_active: bool | None = None
    odds_price: float | None = None
    odds_limit: float | None = None
    odds_changed_at: str | None = None

    # Historical state
    historical_present: bool = False
    historical_entry_count: int | None = None
    historical_active_entry_count: int | None = None
    historical_latest_active: bool | None = None
    historical_latest_price: float | None = None
    historical_latest_limit: float | None = None
    historical_latest_created_at: str | None = None
    historical_latest_active_price: float | None = None
    historical_latest_active_created_at: str | None = None
    historical_opening_price: float | None = None
    historical_opening_created_at: str | None = None
    historical_observed_span_minutes: float | None = None

    # Comparison
    price_delta_latest: float | None = None
    price_delta_latest_active: float | None = None
    price_delta_pct_latest: float | None = None
    price_delta_pct_latest_active: float | None = None
    latest_price_matches: bool | None = None
    latest_active_price_matches: bool | None = None
    active_state_matches: bool | None = None
    timestamp_delta_seconds: float | None = None
    current_only: bool = False
    historical_only: bool = False
    latest_historical_is_inactive: bool = False
    historical_latest_is_stale: bool = False
    comparison_status: str = ""
    diagnostic_reasons: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Aggregate metrics
# ---------------------------------------------------------------------------

@dataclass
class AggregateMetrics:
    current_outcome_count: int = 0
    historical_outcome_count: int = 0
    matched_identity_count: int = 0
    current_coverage_in_historical: float | None = None
    historical_coverage_in_current: float | None = None
    exact_price_match_rate: float | None = None
    tolerance_price_match_rate: float | None = None
    latest_active_price_match_rate: float | None = None
    active_state_agreement_rate: float | None = None
    current_only_count: int = 0
    historical_only_count: int = 0
    latest_inactive_count: int = 0
    stale_historical_count: int = 0
    unmapped_market_count: int = 0
    unmapped_outcome_count: int = 0
    mean_absolute_price_delta: float | None = None
    median_absolute_price_delta: float | None = None
    p95_absolute_price_delta: float | None = None
    maximum_absolute_price_delta: float | None = None
    mean_timestamp_delta: float | None = None
    p95_timestamp_delta: float | None = None
    historical_response_duration: float | None = None
    odds_response_duration: float | None = None
    historical_payload_size: int | None = None
    odds_payload_size: int | None = None


# ---------------------------------------------------------------------------
# Viability assessment
# ---------------------------------------------------------------------------

@dataclass
class ViabilityAssessment:
    historical_only_candidate: bool = False
    blocking_reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metrics_evaluated: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Default viability thresholds
# ---------------------------------------------------------------------------

DEFAULT_VIABILITY_THRESHOLDS = {
    "min_historical_coverage_of_current": 0.99,
    "min_price_match_rate": 0.99,
    "min_active_state_agreement": 0.999,
    "max_current_only_count": 5,
    "max_latest_inactive_as_current": 0,
    "max_p95_timestamp_delta_seconds": 60.0,
}


# ---------------------------------------------------------------------------
# Parsing: /v4/odds
# ---------------------------------------------------------------------------

def parse_odds_response(payload: dict | list | None) -> list[OddsEntry]:
    """Extract flat OddsEntry records from a /v4/odds response."""
    if not isinstance(payload, dict):
        return []

    fixture_id = str(payload.get("fixtureId") or "") or None
    sport_id = str(payload.get("sportId") or "") or None
    entries: list[OddsEntry] = []

    bookmaker_odds = payload.get("bookmakerOdds")
    if not isinstance(bookmaker_odds, dict):
        return entries

    for bookmaker_slug, bookmaker_data in bookmaker_odds.items():
        if not isinstance(bookmaker_data, dict):
            continue
        slug = str(bookmaker_data.get("slug") or bookmaker_slug).strip().lower()
        if not slug:
            continue

        markets = bookmaker_data.get("markets")
        if not isinstance(markets, dict):
            continue

        for market_id, market_data in markets.items():
            if not isinstance(market_data, dict):
                continue
            market_active = market_data.get("marketActive")

            outcomes = market_data.get("outcomes")
            if not isinstance(outcomes, dict):
                continue

            for outcome_id, outcome_data in outcomes.items():
                if not isinstance(outcome_data, dict):
                    continue

                players = outcome_data.get("players")
                if not isinstance(players, dict):
                    continue

                for player_id, player_data in players.items():
                    if not isinstance(player_data, dict):
                        continue

                    entries.append(OddsEntry(
                        fixture_id=fixture_id,
                        sport_id=sport_id,
                        bookmaker=slug,
                        source_market_id=str(market_id),
                        source_outcome_id=str(outcome_id),
                        player_id=str(player_id),
                        market_active=market_active,
                        player_active=player_data.get("active"),
                        price=_safe_float(player_data.get("price")),
                        limit=_safe_float(player_data.get("limit")),
                        changed_at=player_data.get("changedAt"),
                        bookmaker_changed_at=player_data.get("bookmakerChangedAt"),
                        main_line=player_data.get("mainLine"),
                        bookmaker_outcome_id=(
                            str(player_data["bookmakerOutcomeId"])
                            if player_data.get("bookmakerOutcomeId") not in (None, "")
                            else None
                        ),
                    ))

    return entries


# ---------------------------------------------------------------------------
# Parsing: /v4/historical-odds
# ---------------------------------------------------------------------------

def _parse_historical_entries(raw_entries: Any) -> list[HistoricalEntry]:
    """Parse a list (or single dict) of historical quote entries."""
    if isinstance(raw_entries, dict):
        items = [raw_entries]
    elif isinstance(raw_entries, list):
        items = [e for e in raw_entries if isinstance(e, dict)]
    else:
        return []

    result: list[HistoricalEntry] = []
    for item in items:
        raw_ts = item.get("createdAt")
        parsed_ts = _parse_timestamp(raw_ts)
        result.append(HistoricalEntry(
            created_at=parsed_ts,
            created_at_raw=str(raw_ts) if raw_ts is not None else None,
            price=_safe_float(item.get("price")),
            limit=_safe_float(item.get("limit")),
            active=item.get("active"),
            timestamp_valid=parsed_ts is not None,
        ))
    return result


def _sort_historical_entries(
    entries: list[HistoricalEntry],
) -> tuple[list[HistoricalEntry], int]:
    """Sort entries by createdAt ascending. Returns (sorted_valid, invalid_count)."""
    valid: list[tuple[datetime, int, HistoricalEntry]] = []
    invalid_count = 0
    for idx, entry in enumerate(entries):
        if entry.created_at is not None:
            valid.append((entry.created_at, idx, entry))
        else:
            invalid_count += 1
    valid.sort(key=lambda t: (t[0], t[1]))
    return [t[2] for t in valid], invalid_count


def _build_historical_summary(
    fixture_id: str | None,
    bookmaker: str,
    market_id: str,
    outcome_id: str,
    player_id: str,
    raw_entries: Any,
) -> HistoricalSummary:
    """Build a HistoricalSummary from raw historical entries."""
    parsed = _parse_historical_entries(raw_entries)
    sorted_entries, invalid_count = _sort_historical_entries(parsed)

    active_entries = [e for e in sorted_entries if e.active is not False]

    summary = HistoricalSummary(
        fixture_id=fixture_id,
        bookmaker=str(bookmaker).strip().lower(),
        source_market_id=str(market_id),
        source_outcome_id=str(outcome_id),
        player_id=str(player_id),
        total_entry_count=len(parsed),
        active_entry_count=len(active_entries),
        entries=sorted_entries,
        invalid_timestamp_count=invalid_count,
    )

    if sorted_entries:
        summary.latest_entry = sorted_entries[-1]

    if active_entries:
        summary.earliest_active_entry = active_entries[0]
        summary.latest_active_entry = active_entries[-1]

    return summary


def parse_historical_response(
    payload: dict | list | None,
) -> list[HistoricalSummary]:
    """Extract flat HistoricalSummary records from a /v4/historical-odds response."""
    if not isinstance(payload, dict):
        return []

    fixture_id = str(payload.get("fixtureId") or "") or None
    summaries: list[HistoricalSummary] = []

    bookmakers = payload.get("bookmakers")
    if not isinstance(bookmakers, dict):
        return summaries

    for bookmaker_slug, bookmaker_data in bookmakers.items():
        if not isinstance(bookmaker_data, dict):
            continue
        slug = str(bookmaker_slug).strip().lower()
        if not slug:
            continue

        markets = bookmaker_data.get("markets")
        if not isinstance(markets, dict):
            continue

        for market_id, market_data in markets.items():
            if not isinstance(market_data, dict):
                continue

            outcomes = market_data.get("outcomes")
            if not isinstance(outcomes, dict):
                continue

            for outcome_id, outcome_data in outcomes.items():
                if not isinstance(outcome_data, dict):
                    continue

                players = outcome_data.get("players")
                if not isinstance(players, dict):
                    continue

                for player_id, player_entries in players.items():
                    summary = _build_historical_summary(
                        fixture_id=fixture_id,
                        bookmaker=slug,
                        market_id=market_id,
                        outcome_id=outcome_id,
                        player_id=player_id,
                        raw_entries=player_entries,
                    )
                    summaries.append(summary)

    return summaries


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------

def _price_delta(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return round(a - b, 6)


def _price_delta_pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    if b == 0:
        return None
    return round((a - b) / abs(b), 6)


def _prices_match(a: float | None, b: float | None, tolerance: float) -> bool | None:
    if a is None or b is None:
        return None
    return abs(a - b) <= tolerance


def _timestamp_delta_seconds(
    odds_changed_at: str | None,
    historical_created_at: datetime | None,
) -> float | None:
    odds_ts = _parse_timestamp(odds_changed_at)
    if odds_ts is None or historical_created_at is None:
        return None
    return abs((odds_ts - historical_created_at).total_seconds())


def compare_entries(
    odds_entries: list[OddsEntry],
    historical_summaries: list[HistoricalSummary],
    config: ComparisonConfig,
    *,
    enrichment: dict[tuple[str, str], EnrichmentInfo] | None = None,
    fixture_id: str | None = None,
    sport_id: str | None = None,
    minutes_until_start: float | None = None,
    captured_at: str | None = None,
    bookmaker_filter: set[str] | None = None,
) -> list[ComparisonRow]:
    """Full-outer-join comparison of odds vs historical entries.

    ``enrichment`` maps (source_market_id, source_outcome_id) → EnrichmentInfo.
    """
    odds_by_id: dict[tuple, OddsEntry] = {}
    for entry in odds_entries:
        if bookmaker_filter and entry.bookmaker not in bookmaker_filter:
            continue
        odds_by_id[entry.identity] = entry

    hist_by_id: dict[tuple, HistoricalSummary] = {}
    for summary in historical_summaries:
        if bookmaker_filter and summary.bookmaker not in bookmaker_filter:
            continue
        hist_by_id[summary.identity] = summary

    all_ids = set(odds_by_id.keys()) | set(hist_by_id.keys())
    enrichment = enrichment or {}
    rows: list[ComparisonRow] = []

    for identity in sorted(all_ids):
        odds_entry = odds_by_id.get(identity)
        hist_summary = hist_by_id.get(identity)

        bk, mkt, out, pid = identity
        enrich = enrichment.get((mkt, out))

        row = ComparisonRow(
            fixture_id=fixture_id or (
                (odds_entry.fixture_id if odds_entry else None)
                or (hist_summary.fixture_id if hist_summary else None)
            ),
            sport_id=sport_id or (
                odds_entry.sport_id if odds_entry else None
            ),
            minutes_until_start=minutes_until_start,
            captured_at=captured_at,
            bookmaker=bk,
            source_market_id=mkt,
            source_outcome_id=out,
            player_id=pid,
        )

        # Enrichment
        if enrich:
            row.canonical_market_key = enrich.canonical_market_key
            row.canonical_choice_name = enrich.canonical_choice_name
            row.handicap = enrich.handicap

        # /odds state
        if odds_entry:
            row.odds_present = True
            row.odds_market_active = odds_entry.market_active
            row.odds_player_active = odds_entry.player_active
            row.odds_price = odds_entry.price
            row.odds_limit = odds_entry.limit
            row.odds_changed_at = odds_entry.changed_at
            row.main_line = odds_entry.main_line

        # Historical state
        if hist_summary:
            row.historical_present = True
            row.historical_entry_count = hist_summary.total_entry_count
            row.historical_active_entry_count = hist_summary.active_entry_count

            latest = hist_summary.latest_entry
            latest_active = hist_summary.latest_active_entry
            earliest_active = hist_summary.earliest_active_entry

            if latest:
                row.historical_latest_active = latest.active
                row.historical_latest_price = latest.price
                row.historical_latest_limit = latest.limit
                row.historical_latest_created_at = (
                    latest.created_at.isoformat() if latest.created_at else None
                )

            if latest_active:
                row.historical_latest_active_price = latest_active.price
                row.historical_latest_active_created_at = (
                    latest_active.created_at.isoformat()
                    if latest_active.created_at
                    else None
                )

            if earliest_active:
                row.historical_opening_price = earliest_active.price
                row.historical_opening_created_at = (
                    earliest_active.created_at.isoformat()
                    if earliest_active.created_at
                    else None
                )

            if earliest_active and latest_active and earliest_active.created_at and latest_active.created_at:
                span = (latest_active.created_at - earliest_active.created_at).total_seconds()
                row.historical_observed_span_minutes = round(span / 60.0, 2)

        # Comparison deltas
        if odds_entry and hist_summary:
            latest = hist_summary.latest_entry
            latest_active = hist_summary.latest_active_entry

            if latest and latest.price is not None and odds_entry.price is not None:
                row.price_delta_latest = _price_delta(odds_entry.price, latest.price)
                row.price_delta_pct_latest = _price_delta_pct(odds_entry.price, latest.price)
                row.latest_price_matches = _prices_match(
                    odds_entry.price, latest.price, config.price_tolerance
                )

            if latest_active and latest_active.price is not None and odds_entry.price is not None:
                row.price_delta_latest_active = _price_delta(odds_entry.price, latest_active.price)
                row.price_delta_pct_latest_active = _price_delta_pct(odds_entry.price, latest_active.price)
                row.latest_active_price_matches = _prices_match(
                    odds_entry.price, latest_active.price, config.price_tolerance
                )

            # Active state
            if latest:
                odds_active = odds_entry.player_active is not False
                hist_active = latest.active is not False
                row.active_state_matches = odds_active == hist_active

            # Timestamp delta
            if latest and latest.created_at:
                row.timestamp_delta_seconds = _timestamp_delta_seconds(
                    odds_entry.changed_at, latest.created_at
                )

            # Latest-inactive flag
            if latest and latest.active is False:
                row.latest_historical_is_inactive = True

            # Stale check
            if row.timestamp_delta_seconds is not None:
                row.historical_latest_is_stale = (
                    row.timestamp_delta_seconds > config.stale_threshold_seconds
                )

        # Exclusive flags
        if odds_entry and not hist_summary:
            row.current_only = True
        if hist_summary and not odds_entry:
            row.historical_only = True

        # Classify
        status, reasons = _classify(row, config)
        row.comparison_status = status.value
        row.diagnostic_reasons = reasons

        rows.append(row)

    return rows


def _classify(
    row: ComparisonRow,
    config: ComparisonConfig,
) -> tuple[ComparisonStatus, list[str]]:
    """Determine the primary comparison_status and diagnostic_reasons."""
    reasons: list[str] = []

    if row.current_only:
        reasons.append("outcome present only in /odds")
        return ComparisonStatus.CURRENT_ONLY, reasons

    if row.historical_only:
        reasons.append("outcome present only in historical")
        return ComparisonStatus.HISTORICAL_ONLY, reasons

    # Invalid prices
    if row.odds_present and row.odds_price is None:
        reasons.append("current price is None or non-numeric")
        return ComparisonStatus.INVALID_CURRENT_PRICE, reasons

    if row.historical_present and row.historical_latest_price is None:
        if row.historical_entry_count and row.historical_entry_count > 0:
            reasons.append("latest historical price is None or non-numeric")
            return ComparisonStatus.INVALID_HISTORICAL_PRICE, reasons

    # Invalid historical timestamp
    if (
        row.historical_present
        and row.historical_latest_created_at is None
        and row.historical_entry_count
        and row.historical_entry_count > 0
    ):
        reasons.append("latest historical timestamp is invalid")
        return ComparisonStatus.INVALID_HISTORICAL_TIMESTAMP, reasons

    # Latest historical is inactive
    if row.latest_historical_is_inactive:
        reasons.append(
            "latest historical entry is inactive; "
            "latest_active may differ from current state"
        )
        # This is serious enough to be primary status
        primary = ComparisonStatus.HISTORICAL_LATEST_INACTIVE

        # Also check price
        if row.latest_active_price_matches is False:
            reasons.append("latest_active price does not match current")
        if row.active_state_matches is False:
            reasons.append("active state disagrees")
        return primary, reasons

    # Stale historical
    if row.historical_latest_is_stale:
        reasons.append(
            f"timestamp delta {row.timestamp_delta_seconds:.1f}s "
            f"exceeds stale threshold {config.stale_threshold_seconds:.1f}s"
        )
        primary = ComparisonStatus.HISTORICAL_STALE
        if row.latest_price_matches is False:
            reasons.append("price mismatch may be due to stale data or update between requests")
        return primary, reasons

    # Active state mismatch
    if row.active_state_matches is False:
        reasons.append("player active state disagrees between endpoints")
        return ComparisonStatus.ACTIVE_STATE_MISMATCH, reasons

    # Price mismatch
    if row.latest_price_matches is False:
        delta_info = ""
        if row.price_delta_latest is not None:
            delta_info = f" (delta={row.price_delta_latest:.4f})"
        reasons.append(f"latest price mismatch{delta_info}")

        # Check if update happened between sequential requests
        if row.timestamp_delta_seconds is not None and row.timestamp_delta_seconds > 0:
            reasons.append("possible update_between_requests")
        return ComparisonStatus.PRICE_MISMATCH, reasons

    # Match
    if row.latest_price_matches is True:
        return ComparisonStatus.MATCH, reasons

    # Fallback: both present, but no valid price comparison possible
    if row.odds_present and row.historical_present:
        reasons.append("comparison inconclusive: missing price data for delta")
        return ComparisonStatus.MATCH, reasons

    return ComparisonStatus.MATCH, reasons


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _safe_percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    sorted_vals = sorted(values)
    k = (len(sorted_vals) - 1) * (percentile / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return round(d0 + d1, 6)


def _safe_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.mean(values), 6)


def _safe_median(values: list[float]) -> float | None:
    if not values:
        return None
    return round(statistics.median(values), 6)


def _safe_rate(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator, 6)


def aggregate_rows(
    rows: list[ComparisonRow],
    *,
    historical_response_duration: float | None = None,
    odds_response_duration: float | None = None,
    historical_payload_size: int | None = None,
    odds_payload_size: int | None = None,
) -> AggregateMetrics:
    """Compute aggregate metrics over a list of comparison rows."""
    if not rows:
        return AggregateMetrics(
            historical_response_duration=historical_response_duration,
            odds_response_duration=odds_response_duration,
            historical_payload_size=historical_payload_size,
            odds_payload_size=odds_payload_size,
        )

    current_count = sum(1 for r in rows if r.odds_present)
    historical_count = sum(1 for r in rows if r.historical_present)
    matched_count = sum(1 for r in rows if r.odds_present and r.historical_present)
    current_only = sum(1 for r in rows if r.current_only)
    historical_only = sum(1 for r in rows if r.historical_only)
    latest_inactive = sum(1 for r in rows if r.latest_historical_is_inactive)
    stale_count = sum(1 for r in rows if r.historical_latest_is_stale)
    unmapped_market = sum(
        1 for r in rows if r.comparison_status == ComparisonStatus.UNMAPPED_MARKET.value
    )
    unmapped_outcome = sum(
        1 for r in rows if r.comparison_status == ComparisonStatus.UNMAPPED_OUTCOME.value
    )

    # Exact and tolerance price match rates (among matched identities)
    exact_matches = sum(
        1 for r in rows
        if r.odds_present and r.historical_present
        and r.latest_price_matches is True
        and r.price_delta_latest is not None
        and r.price_delta_latest == 0.0
    )
    tolerance_matches = sum(
        1 for r in rows
        if r.latest_price_matches is True
    )
    latest_active_matches = sum(
        1 for r in rows
        if r.latest_active_price_matches is True
    )
    active_agreements = sum(
        1 for r in rows
        if r.active_state_matches is True
    )
    active_comparisons = sum(
        1 for r in rows
        if r.active_state_matches is not None
    )

    # Price deltas for stats
    abs_deltas = [
        abs(r.price_delta_latest)
        for r in rows
        if r.price_delta_latest is not None
    ]
    ts_deltas = [
        r.timestamp_delta_seconds
        for r in rows
        if r.timestamp_delta_seconds is not None
    ]

    return AggregateMetrics(
        current_outcome_count=current_count,
        historical_outcome_count=historical_count,
        matched_identity_count=matched_count,
        current_coverage_in_historical=_safe_rate(matched_count, current_count),
        historical_coverage_in_current=_safe_rate(matched_count, historical_count),
        exact_price_match_rate=_safe_rate(exact_matches, matched_count),
        tolerance_price_match_rate=_safe_rate(tolerance_matches, matched_count),
        latest_active_price_match_rate=_safe_rate(latest_active_matches, matched_count),
        active_state_agreement_rate=_safe_rate(active_agreements, active_comparisons),
        current_only_count=current_only,
        historical_only_count=historical_only,
        latest_inactive_count=latest_inactive,
        stale_historical_count=stale_count,
        unmapped_market_count=unmapped_market,
        unmapped_outcome_count=unmapped_outcome,
        mean_absolute_price_delta=_safe_mean(abs_deltas),
        median_absolute_price_delta=_safe_median(abs_deltas),
        p95_absolute_price_delta=_safe_percentile(abs_deltas, 95),
        maximum_absolute_price_delta=round(max(abs_deltas), 6) if abs_deltas else None,
        mean_timestamp_delta=_safe_mean(ts_deltas),
        p95_timestamp_delta=_safe_percentile(ts_deltas, 95),
        historical_response_duration=historical_response_duration,
        odds_response_duration=odds_response_duration,
        historical_payload_size=historical_payload_size,
        odds_payload_size=odds_payload_size,
    )


def aggregate_by_dimension(
    rows: list[ComparisonRow],
    dimension_fn,
) -> dict[str, AggregateMetrics]:
    """Group rows by a dimension function and compute aggregates per group."""
    groups: dict[str, list[ComparisonRow]] = {}
    for row in rows:
        key = str(dimension_fn(row) or "unknown")
        groups.setdefault(key, []).append(row)
    return {key: aggregate_rows(group) for key, group in sorted(groups.items())}


# ---------------------------------------------------------------------------
# Viability assessment
# ---------------------------------------------------------------------------

def assess_viability(
    aggregate: AggregateMetrics,
    *,
    thresholds: dict[str, float] | None = None,
) -> ViabilityAssessment:
    """Evaluate whether Historical Odds can be the sole source for regular bookmakers."""
    t = {**DEFAULT_VIABILITY_THRESHOLDS, **(thresholds or {})}
    result = ViabilityAssessment()
    result.metrics_evaluated = {
        "current_outcome_count": aggregate.current_outcome_count,
        "historical_outcome_count": aggregate.historical_outcome_count,
        "matched_identity_count": aggregate.matched_identity_count,
        "current_coverage_in_historical": aggregate.current_coverage_in_historical,
        "tolerance_price_match_rate": aggregate.tolerance_price_match_rate,
        "latest_active_price_match_rate": aggregate.latest_active_price_match_rate,
        "active_state_agreement_rate": aggregate.active_state_agreement_rate,
        "current_only_count": aggregate.current_only_count,
        "latest_inactive_count": aggregate.latest_inactive_count,
        "p95_timestamp_delta": aggregate.p95_timestamp_delta,
        "stale_historical_count": aggregate.stale_historical_count,
    }

    can_pass = True

    # Coverage
    coverage = aggregate.current_coverage_in_historical
    min_coverage = t["min_historical_coverage_of_current"]
    if coverage is not None and coverage < min_coverage:
        result.blocking_reasons.append(
            f"historical coverage of current {coverage:.4f} < {min_coverage}"
        )
        can_pass = False
    elif coverage is None:
        result.warnings.append("coverage could not be calculated")

    # Price match rate
    pmr = aggregate.tolerance_price_match_rate
    min_pmr = t["min_price_match_rate"]
    if pmr is not None and pmr < min_pmr:
        result.blocking_reasons.append(
            f"tolerance price match rate {pmr:.4f} < {min_pmr}"
        )
        can_pass = False
    elif pmr is None:
        result.warnings.append("price match rate could not be calculated")

    # Active state
    asr = aggregate.active_state_agreement_rate
    min_asr = t["min_active_state_agreement"]
    if asr is not None and asr < min_asr:
        result.blocking_reasons.append(
            f"active state agreement {asr:.4f} < {min_asr}"
        )
        can_pass = False
    elif asr is None:
        result.warnings.append("active state agreement could not be calculated")

    # Current-only
    max_co = t["max_current_only_count"]
    if aggregate.current_only_count > max_co:
        result.blocking_reasons.append(
            f"current-only outcomes {aggregate.current_only_count} > {max_co}"
        )
        can_pass = False

    # Latest inactive used as current
    max_li = t["max_latest_inactive_as_current"]
    if aggregate.latest_inactive_count > max_li:
        result.blocking_reasons.append(
            f"latest-inactive entries {aggregate.latest_inactive_count} > {max_li}"
        )
        can_pass = False

    # p95 timestamp delta
    max_p95 = t["max_p95_timestamp_delta_seconds"]
    p95 = aggregate.p95_timestamp_delta
    if p95 is not None and p95 > max_p95:
        result.warnings.append(
            f"p95 timestamp delta {p95:.1f}s > {max_p95:.1f}s"
        )
        # Warning, not blocking

    result.historical_only_candidate = can_pass
    return result
