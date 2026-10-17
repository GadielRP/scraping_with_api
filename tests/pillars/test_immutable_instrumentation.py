"""Acquisition boundaries and preservation of actual quantitative components."""

from dataclasses import asdict
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
import pytest
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.market_evaluation import prepare_event_markets
from modules.pillars.trajectory_selection import TargetMinuteSelection
from modules.pillars.pillar_5.run_pillar_5 import prepare_pillar_5, calculate_pillar_5
from modules.pillars.pillar_5.calculation_models import MemorySample
from modules.pillars.pillar_1_team_structure.totals import calculate_p1_totals
from modules.pillars.mining.adapters import P1TotalsMiningAdapter
from modules.pillars.instrumentation import capture_calculations, calculation_scope
from tests.pillars.test_market_evaluation import START, event, quotes
from tests.pillars.test_structural_target_selection import _event_context


def test_late_ingestion_cannot_be_relabelled_as_a_known_historical_tick():
    cutoff = START - timedelta(minutes=5)
    rows = quotes(minute=120) + quotes()
    rows[0]["collected_at"] = cutoff + timedelta(seconds=1)
    rows[1]["collected_at"] = None
    context = build_odds_trajectory_context(
        rows,
        event_starts_at=START,
        target_minutes_expected=[120, 30, 5],
        evaluation_minute=5,
        evaluation_as_of=cutoff,
    )
    assert context.availability_diagnostics == {
        "unknown_availability": 1,
        "after_evaluation_as_of": 1,
    }
    snapshots = [
        p
        for groups in context.markets.values()
        for periods in groups.values()
        for names in periods.values()
        for line in names.values()
        for book in line.bookies.values()
        for choice in book.choices.values()
        for p in choice.snapshots
    ]
    assert len(snapshots) == 4
    assert all(p.collected_at <= cutoff for p in snapshots)


def test_observed_checkpoint_does_not_require_a_recent_source_change():
    from modules.pillars.trajectory_sampling import TrajectorySamplingPolicy

    rows = quotes()
    for row in rows:
        row["source_collected_at"] = START - timedelta(hours=6)
    cutoff = START - timedelta(minutes=5)
    context = build_odds_trajectory_context(
        rows, event_starts_at=START, evaluation_as_of=cutoff, evaluation_minute=5
    )
    assert 5 in context.target_minutes_present
    line = next(
        line
        for periods in context.markets.values()
        for names in periods.values()
        for variants in names.values()
        for line in variants.values()
    )
    choice = next(iter(next(iter(line.bookies.values())).choices.values()))
    sample = TrajectorySamplingPolicy.for_context(context, 5, START).select(
        choice.snapshots
    )
    assert sample.endpoint.collected_at == cutoff
    assert sample.endpoint.effective_at == cutoff
    assert sample.endpoint.source_collected_at == START - timedelta(hours=6)
    assert sample.endpoint.to_dict()["TIME_BASIS"] == "CHECKPOINT_TARGET"


def test_missing_source_clock_uses_observation_without_fabricating_source_time():
    from modules.pillars.trajectory_selection import effective_snapshot_timestamp

    observed = START - timedelta(minutes=5)
    assert (
        effective_snapshot_timestamp(collected_at=observed, source_collected_at=None)
        == observed
    )
    later_source = observed + timedelta(seconds=1)
    assert (
        effective_snapshot_timestamp(
            collected_at=observed, source_collected_at=later_source
        )
        == later_source
    )


def test_existing_snapshot_ids_participate_without_rewriting_timestamps():
    rows = quotes()
    rows[0]["snapshot_id"] = 0
    original = [dict(row) for row in rows]
    context = build_odds_trajectory_context(
        rows, event_starts_at=START, evaluation_as_of=START, evaluation_minute=5
    )
    assert context.availability_diagnostics["unknown_availability"] == 0
    assert context.target_minutes_present == [5]
    assert rows == original


def test_scoring_uses_acquired_population_without_querying_again():
    rows = quotes() + quotes(book=3) + quotes(book=1)
    context = build_odds_trajectory_context(
        rows, event_starts_at=START, evaluation_minute=5
    )
    target = TargetMinuteSelection(5)
    markets = prepare_event_markets(context, target, event())

    class Memory:
        def __init__(self):
            self.calls = 0

        def summarize(self, *, key, **kwargs):
            self.calls += 1
            n = 0 if key.bookie_id == 3 else 3
            return MemorySample(key, n, 0, n, 0)

    memory = Memory()
    captured = prepare_pillar_5(
        event(),
        context,
        target_selection=target,
        market_evaluation=markets,
        memory_repository=memory,
    )
    calls = memory.calls

    def unexpected(**kwargs):
        raise AssertionError("scoring queried history")

    memory.summarize = unexpected
    result = calculate_pillar_5(prepared_inputs=captured)
    assert memory.calls == calls
    assert any(
        p.get("P5_DIRECTION") == "DRAW" and p.get("P5_VALID")
        for p in result["analysis"].values()
    )
    assert any(s["reason"] == "INSUFFICIENT_OBSERVATIONS" for s in result["signals"])
    assert result["status"] == "ACTIVE"


def totals_streak():
    home = [
        {"team_score": i % 4, "opponent_score": (i * 2) % 3, "startTimestamp": 1000 - i}
        for i in range(30)
    ]
    away = [
        {"team_score": (i * 3) % 5, "opponent_score": i % 3, "startTimestamp": 1000 - i}
        for i in range(30)
    ]
    teams = {
        f"T{i}": {
            "team_name": f"T{i}",
            "results": [
                {
                    "team_score": (n + i) % 5,
                    "opponent_score": (n * i + 1) % 4,
                    "startTimestamp": 1000 - n,
                }
                for n in range(30)
            ],
        }
        for i in range(8)
    }
    return SimpleNamespace(
        home_team_results=home,
        away_team_results=away,
        league_totals_context={
            "matches": [
                {"home_score": i % 5, "away_score": (i * 3) % 4} for i in range(50)
            ],
            "teams": teams,
        },
    )


def test_real_totals_composite_can_be_recovered_continuously():
    with capture_calculations() as trace:
        with calculation_scope("totals"):
            event = _event_context()
            output = calculate_p1_totals(
                totals_streak(), event.to_identity(), event.competition
            )
    raw = asdict(output)
    run = P1TotalsMiningAdapter().build(_event_context(), raw)
    metrics = {m.name: m.value for m in run.units[0].metrics}
    expected = (
        0.55 * output.P1_TOTALS_DIRECTIONAL_SCORE
        + 0.30 * output.BREAKOUT_SCORE
        + 0.15 * output.TREND_DOMINANCE
    )
    assert output.P1_TOTALS_COMPOSITE == pytest.approx(max(-1, min(1, expected)))
    for key in (
        "P1_TOTALS_DIRECTIONAL_SCORE",
        "BREAKOUT_SCORE",
        "TREND_DOMINANCE",
        "P1_TOTALS_COMPOSITE",
        "ALIGNMENT_SCORE",
        "UNDER_COUNT",
        "OVER_COUNT",
    ):
        assert Decimal(str(raw[key])) == metrics[key]
    assert run.status == "ACTIVE"
    assert run.output_payload["calculation_status"] == "OK"
    assert run.output_payload["raw"]["composite_breakout"]["weights"] == {
        "directional": 0.55,
        "breakout": 0.30,
        "trend_dominance": 0.15,
    }
    assert trace["totals"][0]["operation"] == "totals_windows"
    assert raw["raw"]["temporal"]["home_window_components"]["SHORT"]["games_used"] > 0
    assert raw["raw"]["trend"]["league_trend_deltas_by_team"]


def test_totals_preserves_completed_components_when_a_later_stage_aborts(monkeypatch):
    from importlib import import_module

    totals = import_module("modules.pillars.pillar_1_team_structure.totals.totals")
    monkeypatch.setattr(totals, "_calculate_league_trend_deltas", lambda *args: {})
    with capture_calculations() as traces:
        with calculation_scope("totals"):
            event = _event_context()
            output = calculate_p1_totals(
                totals_streak(), event.to_identity(), event.competition
            )

    assert output is None
    steps = {step["operation"]: step for step in traces["totals"]}
    assert steps["totals_abort"]["reason"] == "ABORT_TREND_DYNAMIC_SCALE_ZERO"
    assert steps["totals_variance"]["components"]["vol_edge"] is not None
    temporal = steps["totals_temporal"]["components"]
    assert temporal["temporal_profile_score"] is not None
    run = P1TotalsMiningAdapter().build(
        _event_context(),
        {
            "status": "INSUFFICIENT_DATA",
            "reason": "ZERO_DENOMINATOR",
            "calculation_trace": traces["totals"],
        },
    )
    assert any(
        metric.name.endswith(".temporal_profile_score")
        for metric in run.units[0].metrics
    )
    assert run.diagnostics["signals"]["reasons"] == {"ZERO_DENOMINATOR": 1}


def test_effective_code_identity_is_available_in_images_without_git(monkeypatch):
    from modules.pillars.mining import versioning

    def missing_git(*args, **kwargs):
        raise FileNotFoundError("git")

    monkeypatch.setattr(versioning.subprocess, "run", missing_git)
    configuration, versions = versioning.calculation_versions()
    assert versions["commit_sha"] is None
    assert len(versions["code_sha256"]) == 64
    assert versions["configuration_sha256"] == versioning.content_hash(configuration)


def test_input_capture_is_independent_and_storage_reuses_frozen_payload():
    from modules.pillars.mining.serialization import to_json_value

    original = {"observations": [{"odds": Decimal("1.12345678901234567890123456789")}]}
    captured = to_json_value(original)
    original["observations"][0]["odds"] = Decimal("9")
    assert captured["observations"][0]["odds"] == "1.12345678901234567890123456789"
    assert to_json_value(captured, copy_containers=False) is captured


def test_source_after_cutoff_is_excluded_without_clamping():
    rows = quotes()
    cutoff = START - timedelta(minutes=5)
    source = cutoff + timedelta(seconds=1)
    rows[0]["source_collected_at"] = source
    context = build_odds_trajectory_context(
        rows, event_starts_at=START, evaluation_as_of=cutoff, evaluation_minute=5
    )
    assert context.availability_diagnostics["after_evaluation_as_of"] == 1
    assert rows[0]["source_collected_at"] == source


def test_one_late_batch_reconstructs_source_checkpoints_and_preserves_p4_closure():
    from modules.pillars.trajectory_sampling import TrajectorySamplingPolicy
    from modules.pillars.pillar_4.metrics import build_temporal_features

    acquired = START - timedelta(minutes=5)
    base = quotes(outcomes=("1",))[0]
    rows = [
        {
            **base,
            "snapshot_id": index,
            "odds_value": price,
            "collected_at": acquired,
            "source_collected_at": START - timedelta(minutes=minute),
        }
        for index, minute, price in [
            (0, 996, "2.10"),
            (1, 188, "1.65"),
            (2, 121, "1.60"),
            (3, 36, "1.95"),
        ]
    ]
    context = build_odds_trajectory_context(
        rows,
        event_starts_at=START,
        evaluation_as_of=acquired,
        evaluation_minute=5,
        target_minutes_expected=[360, 120, 30, 5],
        tolerance_minutes=0,
    )
    line = next(iter(context.markets["1X2"]["Full Time"].values()))
    choice = next(iter(next(iter(line.values())).bookies.values())).choices["1"]
    assert choice.odds_values == {
        360: Decimal("2.10"),
        120: Decimal("1.60"),
        30: Decimal("1.95"),
        5: Decimal("1.95"),
    }
    sample = TrajectorySamplingPolicy.for_context(context, 5, START).select(
        choice.snapshots
    )
    assert {
        target: point.value for target, point in sample.checkpoints.items()
    } == choice.odds_values
    assert sample.checkpoints[30].snapshot_id == sample.checkpoints[5].snapshot_id == 3
    assert len(choice.snapshots) == 4  # projections do not duplicate stored ingredients
    assert sample.checkpoints[5].source_collected_at == START - timedelta(minutes=36)
    assert sample.checkpoints[5].collected_at == acquired
    legs, _, _ = build_temporal_features(
        [sample.checkpoints[30], sample.checkpoints[5]], expected_target_minutes=[30, 5]
    )
    assert legs[0]["ELAPSED_MINUTES_ACTUAL"] == 25
    assert legs[0]["DELTA_RAW"] == 0
    assert sample.adaptive[-2].effective_at == START - timedelta(minutes=36)
    assert sample.adaptive[-1].effective_at == acquired
    assert len(sample.adaptive) == 5
    adaptive_legs, _, _ = build_temporal_features(sample.adaptive)
    assert adaptive_legs[-2]["ELAPSED_MINUTES_ACTUAL"] == 85
    assert adaptive_legs[-1]["ELAPSED_MINUTES_ACTUAL"] == 31


def test_source_ties_use_observation_and_id_without_losing_microseconds():
    from modules.pillars.trajectory_selection import (
        build_snapshot_target_windows,
        select_checkpoint_snapshots,
    )
    from types import SimpleNamespace

    target = START - timedelta(minutes=5)

    def point(source, collected, snapshot_id):
        return SimpleNamespace(
            source_collected_at=source, collected_at=collected, snapshot_id=snapshot_id
        )

    earlier = target - timedelta(microseconds=2)
    later = target - timedelta(microseconds=1)
    points = [
        point(later, target, 1),
        point(earlier, target, 99),
        point(later, target, 2),
        point(target + timedelta(microseconds=1), target, 100),
        point(later, target - timedelta(microseconds=1), 98),
    ]
    windows = build_snapshot_target_windows(
        event_starts_at=START,
        target_minutes=[5],
        evaluation_as_of=target,
        tolerance_minutes=0,
    )
    assert select_checkpoint_snapshots(points, windows)[5].snapshot_id == 2


def test_observation_fallback_does_not_reconstruct_distant_or_future_checkpoints():
    from modules.pillars.trajectory_sampling import TrajectorySamplingPolicy

    observed = START - timedelta(minutes=5) + timedelta(seconds=20)
    rows = [
        {**row, "collected_at": observed, "source_collected_at": None}
        for row in quotes(outcomes=("1",))
    ]
    context = build_odds_trajectory_context(
        rows,
        event_starts_at=START,
        evaluation_as_of=observed,
        evaluation_minute=5,
        target_minutes_expected=[120, 30, 5, 0],
        tolerance_minutes=3,
    )
    assert context.target_minutes_present == [5]
    choice = next(
        iter(
            next(
                iter(next(iter(context.markets["1X2"]["Full Time"].values())).values())
            ).bookies.values()
        )
    ).choices["1"]
    sample = TrajectorySamplingPolicy.for_context(context, 5, START).select(
        choice.snapshots
    )
    assert set(sample.checkpoints) == {5}
    assert sample.endpoint.effective_at == observed
    assert sample.endpoint.source_collected_at is None
    assert len(sample.adaptive) == 1


def test_source_does_not_complete_a_checkpoint_not_yet_reached():
    rows = quotes(minute=30, outcomes=("1",))
    context = build_odds_trajectory_context(
        rows,
        event_starts_at=START,
        evaluation_as_of=START - timedelta(minutes=6),
        target_minutes_expected=[30, 5],
    )
    assert context.target_minutes_present == [30]
