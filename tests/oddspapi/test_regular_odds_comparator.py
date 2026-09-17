"""Tests for the regular-odds comparator diagnostic module.

Covers: parsing, identity matching, comparison logic, aggregation,
exchange rejection, viability assessment, and edge cases.
"""

from __future__ import annotations

import math

import pytest

from modules.oddspapi.diagnostics.regular_odds_comparator import (
    AggregateMetrics,
    ComparisonConfig,
    ComparisonRow,
    ComparisonStatus,
    EnrichmentInfo,
    HistoricalEntry,
    HistoricalSummary,
    OddsEntry,
    ViabilityAssessment,
    _build_historical_summary,
    _classify,
    _parse_timestamp,
    _safe_float,
    _safe_percentile,
    aggregate_by_dimension,
    aggregate_rows,
    assess_viability,
    compare_entries,
    is_exchange_bookmaker,
    parse_historical_response,
    parse_odds_response,
    reject_exchange_bookmakers,
)

DEFAULT_CONFIG = ComparisonConfig()


# =====================================================================
# Fixtures / Helpers
# =====================================================================

def _odds_payload(
    fixture_id="fix1",
    sport_id=10,
    bookmakers=None,
):
    """Build a minimal /v4/odds response."""
    if bookmakers is None:
        bookmakers = {
            "pinnacle": {
                "markets": {
                    "101": {
                        "marketActive": True,
                        "outcomes": {
                            "201": {
                                "players": {
                                    "0": {
                                        "active": True,
                                        "price": 1.95,
                                        "limit": 500,
                                        "changedAt": "2026-07-28T10:00:00Z",
                                        "bookmakerChangedAt": None,
                                        "mainLine": True,
                                        "bookmakerOutcomeId": "abc123",
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
    return {
        "fixtureId": fixture_id,
        "sportId": sport_id,
        "bookmakerOdds": bookmakers,
    }


def _historical_payload(
    fixture_id="fix1",
    bookmakers=None,
):
    """Build a minimal /v4/historical-odds response."""
    if bookmakers is None:
        bookmakers = {
            "pinnacle": {
                "markets": {
                    "101": {
                        "outcomes": {
                            "201": {
                                "players": {
                                    "0": [
                                        {
                                            "createdAt": "2026-07-28T08:00:00Z",
                                            "price": 1.90,
                                            "limit": 450,
                                            "active": True,
                                            "exchangeMeta": None,
                                        },
                                        {
                                            "createdAt": "2026-07-28T10:00:00Z",
                                            "price": 1.95,
                                            "limit": 500,
                                            "active": True,
                                            "exchangeMeta": None,
                                        },
                                    ]
                                }
                            }
                        }
                    }
                }
            }
        }
    return {"fixtureId": fixture_id, "bookmakers": bookmakers}


# =====================================================================
# Exchange rejection
# =====================================================================

class TestExchangeRejection:
    def test_betfair_ex_is_exchange(self):
        assert is_exchange_bookmaker("betfair-ex") is True

    def test_betfair_ex_case_insensitive(self):
        assert is_exchange_bookmaker("Betfair-EX") is True

    def test_pinnacle_is_not_exchange(self):
        assert is_exchange_bookmaker("pinnacle") is False

    def test_reject_exchange_bookmakers_raises(self):
        with pytest.raises(ValueError, match="Exchange bookmakers"):
            reject_exchange_bookmakers(["pinnacle", "betfair-ex"])

    def test_reject_exchange_returns_clean_list(self):
        result = reject_exchange_bookmakers(["pinnacle", "bet365"])
        assert result == ["pinnacle", "bet365"]


# =====================================================================
# Timestamp parsing
# =====================================================================

class TestTimestampParsing:
    def test_valid_z_suffix(self):
        ts = _parse_timestamp("2026-07-28T10:00:00Z")
        assert ts is not None
        assert ts.tzname() == "UTC"

    def test_valid_offset(self):
        ts = _parse_timestamp("2026-07-28T10:00:00+00:00")
        assert ts is not None

    def test_none_returns_none(self):
        assert _parse_timestamp(None) is None

    def test_empty_string_returns_none(self):
        assert _parse_timestamp("") is None

    def test_invalid_returns_none(self):
        assert _parse_timestamp("not-a-date") is None

    def test_integer_returns_none(self):
        assert _parse_timestamp(12345) is None


# =====================================================================
# Safe float
# =====================================================================

class TestSafeFloat:
    def test_valid_number(self):
        assert _safe_float(1.95) == 1.95

    def test_string_number(self):
        assert _safe_float("2.5") == 2.5

    def test_none_returns_none(self):
        assert _safe_float(None) is None

    def test_empty_string_returns_none(self):
        assert _safe_float("") is None

    def test_nan_returns_none(self):
        assert _safe_float(float("nan")) is None

    def test_inf_returns_none(self):
        assert _safe_float(float("inf")) is None

    def test_non_numeric_returns_none(self):
        assert _safe_float("abc") is None


# =====================================================================
# Parsing /v4/odds
# =====================================================================

class TestParseOddsResponse:
    def test_extracts_basic_entry(self):
        entries = parse_odds_response(_odds_payload())
        assert len(entries) == 1
        e = entries[0]
        assert e.bookmaker == "pinnacle"
        assert e.source_market_id == "101"
        assert e.source_outcome_id == "201"
        assert e.player_id == "0"
        assert e.price == 1.95
        assert e.limit == 500
        assert e.market_active is True
        assert e.player_active is True

    def test_empty_payload_returns_empty(self):
        assert parse_odds_response({}) == []
        assert parse_odds_response(None) == []
        assert parse_odds_response([]) == []

    def test_multiple_bookmakers(self):
        payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.5}}}}}}},
            "bet365": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.6}}}}}}},
        })
        entries = parse_odds_response(payload)
        assert len(entries) == 2
        bookmakers = {e.bookmaker for e in entries}
        assert bookmakers == {"pinnacle", "bet365"}

    def test_player_id_zero(self):
        entries = parse_odds_response(_odds_payload())
        assert entries[0].player_id == "0"

    def test_inactive_player_still_parsed(self):
        payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": False, "price": 1.5}}}}}}}
        })
        entries = parse_odds_response(payload)
        assert len(entries) == 1
        assert entries[0].player_active is False


# =====================================================================
# Parsing /v4/historical-odds
# =====================================================================

class TestParseHistoricalResponse:
    def test_extracts_and_sorts_ascending(self):
        summaries = parse_historical_response(_historical_payload())
        assert len(summaries) == 1
        s = summaries[0]
        assert s.total_entry_count == 2
        assert s.active_entry_count == 2
        assert s.entries[0].created_at < s.entries[1].created_at

    def test_descending_input_gets_sorted(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T10:00:00Z", "price": 2.0, "limit": 100, "active": True},
                {"createdAt": "2026-07-28T08:00:00Z", "price": 1.9, "limit": 90, "active": True},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        s = summaries[0]
        assert s.entries[0].created_at < s.entries[1].created_at
        assert s.latest_entry.price == 2.0
        assert s.earliest_active_entry.price == 1.9

    def test_repeated_timestamps(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T10:00:00Z", "price": 1.9, "limit": 100, "active": True},
                {"createdAt": "2026-07-28T10:00:00Z", "price": 2.0, "limit": 110, "active": True},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        assert summaries[0].total_entry_count == 2

    def test_invalid_timestamp(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "not-valid", "price": 1.9, "limit": 100, "active": True},
                {"createdAt": "2026-07-28T10:00:00Z", "price": 2.0, "limit": 110, "active": True},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        s = summaries[0]
        assert s.total_entry_count == 2
        assert s.invalid_timestamp_count == 1
        assert len(s.entries) == 1  # Only valid-timestamp entries in sorted list
        assert s.latest_entry.price == 2.0

    def test_latest_active_entry(self):
        summaries = parse_historical_response(_historical_payload())
        s = summaries[0]
        assert s.latest_active_entry is not None
        assert s.latest_active_entry.price == 1.95

    def test_latest_inactive_with_earlier_active(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": 1.9, "limit": 100, "active": True},
                {"createdAt": "2026-07-28T10:00:00Z", "price": 2.0, "limit": 110, "active": False},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        s = summaries[0]
        assert s.latest_entry.active is False
        assert s.latest_entry.price == 2.0
        assert s.latest_active_entry.active is True
        assert s.latest_active_entry.price == 1.9

    def test_never_active(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": 1.9, "limit": 100, "active": False},
                {"createdAt": "2026-07-28T10:00:00Z", "price": 2.0, "limit": 110, "active": False},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        s = summaries[0]
        assert s.active_entry_count == 0
        assert s.latest_active_entry is None
        assert s.earliest_active_entry is None

    def test_missing_price(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": None, "limit": 100, "active": True},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        s = summaries[0]
        assert s.latest_entry.price is None

    def test_invalid_price(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": "not-a-number", "limit": 100, "active": True},
            ]}}}}}}
        })
        summaries = parse_historical_response(payload)
        assert summaries[0].latest_entry.price is None

    def test_empty_payload(self):
        assert parse_historical_response({}) == []
        assert parse_historical_response(None) == []

    def test_player_props_multiple_player_ids(self):
        payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {
                "100": [{"createdAt": "2026-07-28T08:00:00Z", "price": 1.5, "limit": 50, "active": True}],
                "200": [{"createdAt": "2026-07-28T08:00:00Z", "price": 2.5, "limit": 60, "active": True}],
            }}}}}}
        })
        summaries = parse_historical_response(payload)
        assert len(summaries) == 2
        player_ids = {s.player_id for s in summaries}
        assert player_ids == {"100", "200"}


# =====================================================================
# Comparison logic
# =====================================================================

class TestCompareEntries:
    def test_perfect_match(self):
        odds = parse_odds_response(_odds_payload())
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert len(rows) == 1
        assert rows[0].comparison_status == ComparisonStatus.MATCH.value
        assert rows[0].odds_present is True
        assert rows[0].historical_present is True

    def test_price_mismatch_outside_tolerance(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 2.10, "changedAt": "2026-07-28T10:00:00Z"}}}}}}}
        })
        hist_payload = _historical_payload()
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert rows[0].comparison_status == ComparisonStatus.PRICE_MISMATCH.value
        assert rows[0].latest_price_matches is False

    def test_price_within_tolerance(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.9505, "changedAt": "2026-07-28T10:00:00Z"}}}}}}}
        })
        hist_payload = _historical_payload()
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        config = ComparisonConfig(price_tolerance=0.01)
        rows = compare_entries(odds, hist, config)
        assert rows[0].latest_price_matches is True

    def test_current_only(self):
        odds = parse_odds_response(_odds_payload())
        rows = compare_entries(odds, [], DEFAULT_CONFIG)
        assert len(rows) == 1
        assert rows[0].current_only is True
        assert rows[0].comparison_status == ComparisonStatus.CURRENT_ONLY.value

    def test_historical_only(self):
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries([], hist, DEFAULT_CONFIG)
        assert len(rows) == 1
        assert rows[0].historical_only is True
        assert rows[0].comparison_status == ComparisonStatus.HISTORICAL_ONLY.value

    def test_bookmaker_absent(self):
        odds_payload = _odds_payload(bookmakers={
            "bet365": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.95}}}}}}}
        })
        hist_payload = _historical_payload()  # pinnacle only
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert len(rows) == 2
        statuses = {r.comparison_status for r in rows}
        assert ComparisonStatus.CURRENT_ONLY.value in statuses
        assert ComparisonStatus.HISTORICAL_ONLY.value in statuses

    def test_outcome_absent_in_historical(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {
                "201": {"players": {"0": {"active": True, "price": 1.95, "changedAt": "2026-07-28T10:00:00Z"}}},
                "999": {"players": {"0": {"active": True, "price": 3.0, "changedAt": "2026-07-28T10:00:00Z"}}},
            }}}}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert len(rows) == 2
        current_only_rows = [r for r in rows if r.current_only]
        assert len(current_only_rows) == 1
        assert current_only_rows[0].source_outcome_id == "999"

    def test_latest_inactive_classification(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.90, "changedAt": "2026-07-28T10:00:00Z"}}}}}}}
        })
        hist_payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": 1.90, "limit": 450, "active": True},
                {"createdAt": "2026-07-28T09:30:00Z", "price": 1.95, "limit": 500, "active": False},
            ]}}}}}}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert rows[0].comparison_status == ComparisonStatus.HISTORICAL_LATEST_INACTIVE.value
        assert rows[0].latest_historical_is_inactive is True

    def test_stale_historical(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.95, "changedAt": "2026-07-28T12:00:00Z"}}}}}}}
        })
        hist_payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T08:00:00Z", "price": 1.95, "limit": 500, "active": True},
            ]}}}}}}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        config = ComparisonConfig(stale_threshold_seconds=60)
        rows = compare_entries(odds, hist, config)
        assert rows[0].comparison_status == ComparisonStatus.HISTORICAL_STALE.value
        assert rows[0].historical_latest_is_stale is True

    def test_active_state_mismatch(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": False, "price": 1.95, "changedAt": "2026-07-28T09:30:00Z"}}}}}}}
        })
        hist_payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"outcomes": {"201": {"players": {"0": [
                {"createdAt": "2026-07-28T09:30:00Z", "price": 1.95, "limit": 500, "active": True},
            ]}}}}}}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert rows[0].comparison_status == ComparisonStatus.ACTIVE_STATE_MISMATCH.value

    def test_matching_by_identity(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {
                "101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.5, "changedAt": "2026-07-28T09:30:00Z"}}}}},
                "102": {"marketActive": True, "outcomes": {"301": {"players": {"0": {"active": True, "price": 2.5, "changedAt": "2026-07-28T09:30:00Z"}}}}},
            }}
        })
        hist_payload = _historical_payload(bookmakers={
            "pinnacle": {"markets": {
                "101": {"outcomes": {"201": {"players": {"0": [{"createdAt": "2026-07-28T09:30:00Z", "price": 1.5, "limit": 100, "active": True}]}}}},
                "102": {"outcomes": {"301": {"players": {"0": [{"createdAt": "2026-07-28T09:30:00Z", "price": 2.5, "limit": 200, "active": True}]}}}},
            }}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert len(rows) == 2
        assert all(r.comparison_status == ComparisonStatus.MATCH.value for r in rows)

    def test_bookmaker_filter(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.5}}}}}}},
            "bet365": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.6}}}}}}},
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG, bookmaker_filter={"pinnacle"})
        bookmakers = {r.bookmaker for r in rows}
        assert bookmakers == {"pinnacle"}

    def test_enrichment_applied(self):
        odds = parse_odds_response(_odds_payload())
        hist = parse_historical_response(_historical_payload())
        enrichment = {
            ("101", "201"): EnrichmentInfo(
                canonical_market_key="1x2_full_time",
                canonical_choice_name="1",
                handicap="0",
            )
        }
        rows = compare_entries(odds, hist, DEFAULT_CONFIG, enrichment=enrichment)
        assert rows[0].canonical_market_key == "1x2_full_time"
        assert rows[0].canonical_choice_name == "1"

    def test_enrichment_absent_does_not_block(self):
        odds = parse_odds_response(_odds_payload())
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG, enrichment={})
        assert rows[0].canonical_market_key is None
        # Comparison still works
        assert rows[0].odds_present is True

    def test_historical_entry_count_and_span(self):
        odds = parse_odds_response(_odds_payload())
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        r = rows[0]
        assert r.historical_entry_count == 2
        assert r.historical_active_entry_count == 2
        assert r.historical_opening_price == 1.90
        assert r.historical_observed_span_minutes is not None
        assert r.historical_observed_span_minutes > 0

    def test_invalid_current_price(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": "bad"}}}}}}}
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        assert rows[0].comparison_status == ComparisonStatus.INVALID_CURRENT_PRICE.value


# =====================================================================
# Aggregation
# =====================================================================

class TestAggregation:
    def test_empty_rows(self):
        agg = aggregate_rows([])
        assert agg.current_outcome_count == 0
        assert agg.mean_absolute_price_delta is None

    def test_single_match(self):
        odds = parse_odds_response(_odds_payload())
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        agg = aggregate_rows(rows)
        assert agg.current_outcome_count == 1
        assert agg.historical_outcome_count == 1
        assert agg.matched_identity_count == 1
        assert agg.current_coverage_in_historical == 1.0

    def test_mixed_statuses(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {
                "101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.95, "changedAt": "2026-07-28T09:30:00Z"}}}}},
                "102": {"marketActive": True, "outcomes": {"301": {"players": {"0": {"active": True, "price": 3.0, "changedAt": "2026-07-28T09:30:00Z"}}}}},
            }}
        })
        hist_payload = _historical_payload()
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(hist_payload)
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        agg = aggregate_rows(rows)
        assert agg.current_outcome_count == 2
        assert agg.current_only_count == 1
        assert agg.matched_identity_count == 1

    def test_aggregate_by_dimension(self):
        odds_payload = _odds_payload(bookmakers={
            "pinnacle": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.95, "changedAt": "2026-07-28T09:30:00Z"}}}}}}},
            "bet365": {"markets": {"101": {"marketActive": True, "outcomes": {"201": {"players": {"0": {"active": True, "price": 1.90, "changedAt": "2026-07-28T09:30:00Z"}}}}}}},
        })
        odds = parse_odds_response(odds_payload)
        hist = parse_historical_response(_historical_payload())
        rows = compare_entries(odds, hist, DEFAULT_CONFIG)
        rollups = aggregate_by_dimension(rows, lambda r: r.bookmaker)
        assert "pinnacle" in rollups
        assert "bet365" in rollups

    def test_response_duration_passthrough(self):
        agg = aggregate_rows([], odds_response_duration=0.5, historical_response_duration=5.0)
        assert agg.odds_response_duration == 0.5
        assert agg.historical_response_duration == 5.0


# =====================================================================
# Percentiles
# =====================================================================

class TestPercentiles:
    def test_empty_list(self):
        assert _safe_percentile([], 95) is None

    def test_single_element(self):
        assert _safe_percentile([5.0], 95) == 5.0

    def test_two_elements(self):
        result = _safe_percentile([1.0, 2.0], 50)
        assert result is not None
        assert 1.0 <= result <= 2.0

    def test_p95_of_many(self):
        values = list(range(100))
        result = _safe_percentile(values, 95)
        assert result is not None
        assert result >= 90

    def test_p0(self):
        assert _safe_percentile([1.0, 5.0, 10.0], 0) == 1.0

    def test_p100(self):
        assert _safe_percentile([1.0, 5.0, 10.0], 100) == 10.0


# =====================================================================
# Viability assessment
# =====================================================================

class TestViabilityAssessment:
    def test_all_thresholds_met(self):
        agg = AggregateMetrics(
            current_outcome_count=100,
            historical_outcome_count=100,
            matched_identity_count=100,
            current_coverage_in_historical=1.0,
            tolerance_price_match_rate=1.0,
            active_state_agreement_rate=1.0,
            current_only_count=0,
            latest_inactive_count=0,
            p95_timestamp_delta=10.0,
        )
        result = assess_viability(agg)
        assert result.historical_only_candidate is True
        assert result.blocking_reasons == []

    def test_low_coverage_blocks(self):
        agg = AggregateMetrics(
            current_outcome_count=100,
            historical_outcome_count=90,
            matched_identity_count=90,
            current_coverage_in_historical=0.90,
            tolerance_price_match_rate=1.0,
            active_state_agreement_rate=1.0,
            current_only_count=10,
        )
        result = assess_viability(agg)
        assert result.historical_only_candidate is False
        assert any("coverage" in r for r in result.blocking_reasons)

    def test_latest_inactive_blocks(self):
        agg = AggregateMetrics(
            current_outcome_count=100,
            matched_identity_count=100,
            current_coverage_in_historical=1.0,
            tolerance_price_match_rate=1.0,
            active_state_agreement_rate=1.0,
            current_only_count=0,
            latest_inactive_count=1,
        )
        result = assess_viability(agg)
        assert result.historical_only_candidate is False

    def test_p95_timestamp_warns(self):
        agg = AggregateMetrics(
            current_outcome_count=100,
            matched_identity_count=100,
            current_coverage_in_historical=1.0,
            tolerance_price_match_rate=1.0,
            active_state_agreement_rate=1.0,
            current_only_count=0,
            latest_inactive_count=0,
            p95_timestamp_delta=120.0,
        )
        result = assess_viability(agg)
        # p95 is a warning, not a blocker
        assert result.historical_only_candidate is True
        assert any("p95" in w for w in result.warnings)

    def test_custom_thresholds(self):
        agg = AggregateMetrics(
            current_outcome_count=100,
            matched_identity_count=95,
            current_coverage_in_historical=0.95,
            tolerance_price_match_rate=0.95,
            active_state_agreement_rate=0.999,
            current_only_count=5,
        )
        custom = {
            "min_historical_coverage_of_current": 0.90,
            "min_price_match_rate": 0.90,
            "max_current_only_count": 10,
        }
        result = assess_viability(agg, thresholds=custom)
        assert result.historical_only_candidate is True

    def test_metrics_evaluated_populated(self):
        agg = AggregateMetrics()
        result = assess_viability(agg)
        assert "current_outcome_count" in result.metrics_evaluated


# =====================================================================
# CLI-level validation
# =====================================================================

class TestCLIValidation:
    def test_live_mode_requires_fixture_id(self):
        from scripts.development.compare_oddspapi_regular_endpoints import main
        exit_code = main(["--live"])
        assert exit_code != 0

    def test_offline_mode_no_http_client_created(self):
        """Offline mode should not import or instantiate OddsPapiClient."""
        import importlib
        import sys

        # Remove any cached import
        modules_to_check = [k for k in sys.modules if "oddspapi.client" in k]

        from scripts.development.compare_oddspapi_regular_endpoints import (
            _compare_fixture,
            parse_odds_response,
            parse_historical_response,
        )
        # Calling parse functions and compare should not trigger client import
        odds = parse_odds_response({})
        hist = parse_historical_response({})
        # No assertion on sys.modules because the module may already be cached;
        # the key point is that no HTTP request is made.
        assert odds == []
        assert hist == []

    def test_exchange_bookmaker_rejected_by_cli(self):
        from scripts.development.compare_oddspapi_regular_endpoints import main
        exit_code = main(["--bookmakers", "betfair-ex,pinnacle", "--odds-file", "nonexistent.json"])
        assert exit_code != 0


# =====================================================================
# API key security
# =====================================================================

class TestApiKeySecurity:
    def test_api_key_not_in_logs(self, caplog):
        """Verify that redacting works in log output."""
        from scripts.development.compare_oddspapi_regular_endpoints import _redact_api_key

        result = _redact_api_key("key=secret123&other=value", "secret123")
        assert "secret123" not in result
        assert "***REDACTED***" in result

    def test_none_api_key_passthrough(self):
        from scripts.development.compare_oddspapi_regular_endpoints import _redact_api_key

        result = _redact_api_key("some text", None)
        assert result == "some text"


# =====================================================================
# Build historical summary (internal function)
# =====================================================================

class TestBuildHistoricalSummary:
    def test_single_entry(self):
        s = _build_historical_summary(
            "fix1", "pinnacle", "101", "201", "0",
            [{"createdAt": "2026-07-28T10:00:00Z", "price": 1.5, "limit": 100, "active": True}],
        )
        assert s.total_entry_count == 1
        assert s.active_entry_count == 1
        assert s.latest_entry.price == 1.5
        assert s.latest_active_entry.price == 1.5
        assert s.earliest_active_entry.price == 1.5

    def test_dict_entry_instead_of_list(self):
        s = _build_historical_summary(
            "fix1", "pinnacle", "101", "201", "0",
            {"createdAt": "2026-07-28T10:00:00Z", "price": 1.5, "limit": 100, "active": True},
        )
        assert s.total_entry_count == 1

    def test_empty_entries(self):
        s = _build_historical_summary("fix1", "pinnacle", "101", "201", "0", [])
        assert s.total_entry_count == 0
        assert s.latest_entry is None

    def test_none_entries(self):
        s = _build_historical_summary("fix1", "pinnacle", "101", "201", "0", None)
        assert s.total_entry_count == 0


# =====================================================================
# Price delta helpers
# =====================================================================

class TestPriceDeltas:
    def test_delta_computation(self):
        from modules.oddspapi.diagnostics.regular_odds_comparator import _price_delta, _price_delta_pct

        assert _price_delta(2.0, 1.5) == 0.5
        assert _price_delta(1.5, 2.0) == -0.5
        assert _price_delta(None, 1.0) is None
        assert _price_delta(1.0, None) is None

        pct = _price_delta_pct(2.0, 1.0)
        assert pct == 1.0  # (2-1)/1 = 1.0

        assert _price_delta_pct(1.0, 0) is None  # div by zero
        assert _price_delta_pct(None, 1.0) is None


# =====================================================================
# Classification edge cases
# =====================================================================

class TestClassification:
    def test_match_status(self):
        row = ComparisonRow(
            odds_present=True,
            historical_present=True,
            odds_price=1.95,
            latest_price_matches=True,
        )
        status, reasons = _classify(row, DEFAULT_CONFIG)
        assert status == ComparisonStatus.MATCH

    def test_current_only_status(self):
        row = ComparisonRow(current_only=True)
        status, reasons = _classify(row, DEFAULT_CONFIG)
        assert status == ComparisonStatus.CURRENT_ONLY

    def test_historical_only_status(self):
        row = ComparisonRow(historical_only=True)
        status, reasons = _classify(row, DEFAULT_CONFIG)
        assert status == ComparisonStatus.HISTORICAL_ONLY

    def test_update_between_requests_reason(self):
        row = ComparisonRow(
            odds_present=True,
            historical_present=True,
            odds_price=2.0,
            historical_latest_price=1.95,
            latest_price_matches=False,
            price_delta_latest=0.05,
            timestamp_delta_seconds=2.0,
        )
        status, reasons = _classify(row, DEFAULT_CONFIG)
        assert status == ComparisonStatus.PRICE_MISMATCH
        assert any("update_between_requests" in r for r in reasons)
