from __future__ import annotations

from decimal import Decimal

from modules.pillars.odds_trajectory_context import build_odds_trajectory_context


def _make_rows() -> list[dict[str, object]]:
    return [
        {
            "event_id": 1,
            "market_id": 10,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "source": "sofascore",
            "exchange_side": None,
            "exchange_level": 0,
            "quote_id": 100,
            "main_line": True,
            "choice_id": 101,
            "choice_name": "1",
            "initial_odds": "1.900",
            "odds_value": "1.850",
            "snapshot_id": 1001,
            "source_collected_at": "2026-01-01T09:59:30",
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 1,
            "target_minute": 1,
            "distance_from_target": 0,
        },
        {
            "event_id": 1,
            "market_id": 11,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 2,
            "bookie_name": "Pinnacle",
            "source": "oddspapi",
            "exchange_side": None,
            "exchange_level": 0,
            "quote_id": 200,
            "main_line": True,
            "choice_id": 201,
            "choice_name": "1",
            "initial_odds": "1.910",
            "odds_value": "1.860",
            "snapshot_id": 2001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 1,
            "target_minute": 1,
            "distance_from_target": 0,
        },
    ]


def test_filter_by_bookie_ids_keeps_only_requested_bookie() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[1])

    filtered = context.filter_by_bookie_ids({1})

    assert filtered.available is True
    assert filtered.target_minutes_present == [1]
    assert filtered.missing_target_minutes == []

    original_bookies = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"].bookies
    )
    filtered_bookies = (
        filtered.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"].bookies
    )

    assert set(original_bookies.keys()) == {
        "1:sofascore:single:0",
        "2:oddspapi:single:0",
    }
    assert set(filtered_bookies.keys()) == {"1:sofascore:single:0"}
    assert all(bookie.bookie_name != "Pinnacle" for bookie in filtered_bookies.values())
    assert set(original_bookies.keys()) == {
        "1:sofascore:single:0",
        "2:oddspapi:single:0",
    }


def test_meta_by_minute_exposes_source_collected_at_as_changed_at() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[1])

    meta = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
        .meta_by_minute[1]
    )

    assert meta.collected_at.isoformat() == "2026-01-01T10:00:00"
    assert meta.changed_at.isoformat() == "2026-01-01T09:59:30"


def test_choice_context_preserves_main_line() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[1])

    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert choice.main_line is True
    assert (
        context.to_dict()["markets"]["1X2"]["Full Time"]["1X2 Full Time"]
        ["__default__"]["bookies"]["1:sofascore:single:0"]["choices"]["1"]["main_line"]
        is True
    )


def test_exchange_size_is_kept_in_exchange_snapshot_metadata_only() -> None:
    rows = _make_rows() + [
        {
            **_make_rows()[0],
            "bookie_id": 9,
            "bookie_name": "Betfair Exchange",
            "source": "oddspapi",
            "exchange_side": "back",
            "exchange_level": 0,
            "quote_id": 900,
            "snapshot_id": 9001,
            "exchange_size": "25.500",
        }
    ]

    context = build_odds_trajectory_context(rows, target_minutes_expected=[1])
    market_line = context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
    regular = context.to_dict()["markets"]["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]["bookies"]["1:sofascore:single:0"]
    exchange = context.to_dict()["markets"]["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]["bookies"]["9:oddspapi:back:0"]

    assert market_line.bookies["9:oddspapi:back:0"].choices["1"].meta_by_minute[1].exchange_size == Decimal("25.500")
    assert "exchange_side" not in regular
    assert "exchange_level" not in regular
    assert exchange["exchange_side"] == "back"
    assert exchange["exchange_level"] == 0
    assert exchange["choices"]["1"]["meta_by_minute"][1]["exchange_size"] == Decimal("25.500")


def test_minute_maps_are_serialized_in_descending_order() -> None:
    rows = []
    for minute in (1, 120, -5, 30, 5):
        rows.append(
            {
                **_make_rows()[0],
                "target_minute": minute,
                "minutes_before_start": minute,
                "snapshot_id": 2000 + minute,
                "quote_id": 3000 + minute,
                "choice_id": 4000 + minute,
                "odds_value": f"{2 + minute / 1000:.3f}",
            }
        )

    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[120, 30, 5, 1, -5],
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert list(choice.meta_by_minute) == [120, 30, 5, 1, -5]
    assert list(choice.odds_values) == [120, 30, 5, 1, -5]


def test_filter_by_bookie_ids_returns_unavailable_context_when_no_bookie_matches() -> None:
    context = build_odds_trajectory_context(_make_rows(), target_minutes_expected=[1])

    filtered = context.filter_by_bookie_ids({999})

    assert filtered.available is False
    assert filtered.markets == {}
    assert filtered.target_minutes_present == []
    assert filtered.missing_target_minutes == [1]


def test_market_group_and_period_filters_still_preserve_shape_and_availability() -> None:
    rows = _make_rows() + [
        {
            "event_id": 1,
            "market_id": 12,
            "market_name": "1X2 Full Time",
            "market_group": "1X2",
            "market_period": "1st half",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "source": "sofascore",
            "exchange_side": None,
            "exchange_level": 0,
            "quote_id": 101,
            "choice_id": 102,
            "choice_name": "1",
            "initial_odds": "2.010",
            "odds_value": "1.970",
            "snapshot_id": 3001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 1,
            "target_minute": 1,
            "distance_from_target": 0,
        },
        {
            "event_id": 1,
            "market_id": 13,
            "market_name": "Over/Under Full Time",
            "market_group": "totals",
            "market_period": "Full Time",
            "choice_group": None,
            "bookie_id": 1,
            "bookie_name": "SofaScore",
            "source": "sofascore",
            "exchange_side": None,
            "exchange_level": 0,
            "quote_id": 102,
            "choice_id": 103,
            "choice_name": "over",
            "initial_odds": "1.750",
            "odds_value": "1.720",
            "snapshot_id": 4001,
            "collected_at": "2026-01-01T10:00:00",
            "minutes_before_start": 1,
            "target_minute": 1,
            "distance_from_target": 0,
        },
    ]

    context = build_odds_trajectory_context(rows, target_minutes_expected=[1])

    filtered = context.filter_by_market_groups({"1X2"}).filter_by_market_period({"Full Time"})

    assert filtered.available is True
    assert set(filtered.markets.keys()) == {"1X2"}
    assert set(filtered.markets["1X2"].keys()) == {"Full Time"}
    assert set(filtered.target_minutes_present) == {1}
    assert filtered.missing_target_minutes == []


def test_multi_source_exchange_series_do_not_collide() -> None:
    rows = []
    quote_id = 500
    for source in ("oddsportal", "oddspapi"):
        for side in ("back", "lay"):
            rows.append(
                {
                    **_make_rows()[0],
                    "bookie_id": 9,
                    "bookie_name": "Betfair Exchange",
                    "source": source,
                    "exchange_side": side,
                    "exchange_level": 0,
                    "quote_id": quote_id,
                    "snapshot_id": quote_id + 1000,
                    "odds_value": str(Decimal("2") + Decimal(quote_id) / 1000),
                }
            )
            quote_id += 1

    context = build_odds_trajectory_context(rows, target_minutes_expected=[1])
    bookies = context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"].bookies

    assert set(bookies) == {
        "9:oddspapi:back:0",
        "9:oddspapi:lay:0",
        "9:oddsportal:back:0",
        "9:oddsportal:lay:0",
    }
    assert {bookie.choices["1"].quote_id for bookie in bookies.values()} == {
        500,
        501,
        502,
        503,
    }
    assert {
        bookie.choices["1"].meta_by_minute[1].quote_id
        for bookie in bookies.values()
    } == {500, 501, 502, 503}


def test_choice_keeps_all_snapshots_and_projects_best_configured_target() -> None:
    base = _make_rows()[1]
    rows = [
        {
            **base,
            "snapshot_id": 2101,
            "odds_value": "2.100",
            "collected_at": "2026-01-01T11:55:10",
            "source_collected_at": "2026-01-01T11:40:00",
            "observed_minutes_before_start": 5,
            "trajectory_minutes_before_start": "20.5041666667",
            "source_limit": "100.500",
        },
        {
            **base,
            "snapshot_id": 2102,
            "odds_value": "2.200",
            "collected_at": "2026-01-01T11:55:20",
            "source_collected_at": "2026-01-01T11:50:00",
            "observed_minutes_before_start": 5,
            "trajectory_minutes_before_start": "10.125",
            "source_limit": "90.250",
        },
    ]

    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[5],
        tolerance_minutes=0,
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["2:oddspapi:single:0"]
        .choices["1"]
    )

    assert [snapshot.snapshot_id for snapshot in choice.snapshots] == [2101, 2102]
    assert choice.odds_values == {5: Decimal("2.200")}
    assert choice.meta_by_minute[5].snapshot_id == 2102
    assert choice.snapshots[0].minutes_before_start == Decimal("20.5041666667")
    assert choice.snapshots[0].source_limit == Decimal("100.500")
    serialized = context.to_dict()["markets"]["1X2"]["Full Time"][
        "1X2 Full Time"
    ]["__default__"]["bookies"]["2:oddspapi:single:0"]["choices"]["1"]
    assert len(serialized["snapshots"]) == 2
    assert serialized["snapshots"][1]["snapshot_id"] == 2102
    assert "source_minutes_before_start" not in serialized["snapshots"][0]


def test_projection_prefers_fresher_provider_tick_before_ingestion_time() -> None:
    base = _make_rows()[1]
    rows = [
        {
            **base,
            "snapshot_id": 2201,
            "odds_value": "1.819",
            # The opening was persisted during the T-5 ingestion batch.
            "collected_at": "2026-05-19T17:00:20",
            "source_collected_at": "2026-05-18T20:00:00",
            "observed_minutes_before_start": 5,
            "trajectory_minutes_before_start": "1265.0",
        },
        {
            **base,
            "snapshot_id": 2202,
            "odds_value": "1.990",
            # The T-5 moment/current was persisted slightly earlier, but its
            # provider timestamp represents the fresher market state.
            "collected_at": "2026-05-19T17:00:00",
            "source_collected_at": "2026-05-19T16:58:58",
            "observed_minutes_before_start": 5,
            "trajectory_minutes_before_start": "6.0333333333",
        },
    ]

    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[5],
        tolerance_minutes=0,
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["2:oddspapi:single:0"]
        .choices["1"]
    )

    assert [snapshot.snapshot_id for snapshot in choice.snapshots] == [2201, 2202]
    assert choice.odds_values == {5: Decimal("1.990")}
    assert choice.meta_by_minute[5].snapshot_id == 2202


def test_snapshot_minutes_and_target_projection_use_independent_time_axes() -> None:
    row = {
        **_make_rows()[0],
        "observed_minutes_before_start": 5,
        "trajectory_minutes_before_start": "20.5041666667",
        "minutes_before_start": None,
        "odds_value": "1.875",
    }

    context = build_odds_trajectory_context(
        [row],
        target_minutes_expected=[5],
        tolerance_minutes=0,
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert choice.snapshots[0].minutes_before_start == Decimal("20.5041666667")
    assert choice.odds_values == {5: Decimal("1.875")}
    assert choice.meta_by_minute[5].minutes_before_start == 5


def test_previous_payload_prefers_source_minutes_for_snapshot_timeline() -> None:
    row = {
        **_make_rows()[0],
        "minutes_before_start": 5,
        "source_minutes_before_start": "20.25",
        "odds_value": "1.875",
    }

    context = build_odds_trajectory_context(
        [row],
        target_minutes_expected=[5],
        tolerance_minutes=0,
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert choice.snapshots[0].minutes_before_start == Decimal("20.25")
    assert choice.meta_by_minute[5].minutes_before_start == 5


def test_evaluation_minute_excludes_later_configured_targets() -> None:
    rows = [
        {
            **_make_rows()[0],
            "snapshot_id": 3000 + minute,
            "minutes_before_start": minute,
            "odds_value": str(Decimal("2") + Decimal(minute) / 100),
        }
        for minute in (5, 0, -5)
    ]

    context = build_odds_trajectory_context(
        rows,
        target_minutes_expected=[5, 0, -5],
        tolerance_minutes=0,
        evaluation_minute=5,
    )
    choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert len(choice.snapshots) == 3
    assert choice.odds_values == {5: Decimal("2.05")}
    assert context.target_minutes_present == [5]
    assert context.missing_target_minutes == [0, -5]


def test_filtered_context_copies_snapshot_lists() -> None:
    context = build_odds_trajectory_context(
        _make_rows(),
        target_minutes_expected=[1],
    )
    filtered = context.filter_by_bookie_ids({1})
    original_choice = (
        context.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )
    filtered_choice = (
        filtered.markets["1X2"]["Full Time"]["1X2 Full Time"]["__default__"]
        .bookies["1:sofascore:single:0"]
        .choices["1"]
    )

    assert filtered_choice.snapshots == original_choice.snapshots
    assert filtered_choice.snapshots is not original_choice.snapshots
