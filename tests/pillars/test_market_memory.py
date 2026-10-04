"""Verify event-scoped sharing and bounded historical population memory."""

import gc
import json
import tracemalloc
import pytest

from sqlalchemy import text

from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    Pillar5PriceMemoryRepository,
)
from modules.pillars.trajectory_sampling import TrajectoryPointValue
from modules.pillars.pillar_4.trajectory_policy import extract_p4_trajectory_inputs
from modules.pillars.pillar_4.trajectory_engine import build_trajectory_features
from modules.pillars.odds_trajectory_context import build_odds_trajectory_context
from modules.pillars.trajectory_selection import TargetMinuteSelection
from modules.pillars.pillar_5.calculation_models import PopulationFilters
from tests.pillars.test_market_evaluation import START, quotes, event
from tests.test_p5_audit_repository import manager, key


def test_p4_features_retain_points_and_derived_views_share_provenance():
    rows = quotes(minute=120) + quotes(minute=30) + quotes()
    context = build_odds_trajectory_context(
        rows, target_minutes_expected=[120, 30, 5], event_starts_at=START
    )
    extraction = extract_p4_trajectory_inputs(
        event(), context, TargetMinuteSelection(5)
    )
    features = build_trajectory_features(
        extraction.adaptive_series, apply_relations=False
    )
    assert all(
        result.points is source.points
        for result, source in zip(features, extraction.adaptive_series)
    )
    price = [
        series
        for series in extraction.adaptive_series
        if series.value_type == "ODDS_PRICE"
    ]
    derived = [
        series
        for series in extraction.adaptive_series
        if series.value_type == "IMPLIED_PROBABILITY_RAW"
    ]
    assert price and derived
    for series in derived:
        assert all(isinstance(point, TrajectoryPointValue) for point in series.points)
        source = next(
            item for item in price if item.base_series_id == series.base_series_id
        )
        assert all(
            point.original is original
            for point, original in zip(series.points, source.points)
        )


@pytest.mark.parametrize("capture", [False, True])
def test_p5_all_population_is_counted_without_linear_python_memory(manager, capture):

    # Generate the population inside SQL, outside Python's heap and payload.
    def measure(size):
        with manager.get_session() as session:
            session.execute(text("DELETE FROM mv_p5_price_memory"))
            session.execute(
                text("""WITH RECURSIVE ids(id) AS (
                SELECT 1 UNION ALL SELECT id+1 FROM ids WHERE id < :n
            ) INSERT INTO mv_p5_price_memory
              SELECT id,'Football',11,2026,'Mexico',302,'1X2','Full Time',1,
                     '2026-09-01 18:00:00.000000',2,2,2,1,0,'HOME','2026-09-02 18:00:00.000000'
              FROM ids"""),
                {"n": size},
            )
        gc.collect()
        tracemalloc.start()
        with manager.get_session() as session:
            repository = Pillar5PriceMemoryRepository(session=session, capture=capture)
            summary = repository.summarize(
                key=key(),
                current_event_id=900000,
                current_starts_at=START,
                population_filters=PopulationFilters(),
            )
            session.rollback()
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert summary.sample_size == size
        assert not hasattr(summary, "historical_matches")
        payload_size = len(
            json.dumps(
                {
                    "n": summary.sample_size,
                    "wins": summary.wins_home,
                    "exclusions": summary.exclusions,
                }
            )
        )
        return peak, payload_size

    measure(10)  # Warm SQL compilation and dialect caches.
    small_peak, small_payload = measure(1000)
    large_peak, large_payload = measure(50000)
    assert large_peak < small_peak + 300000
    assert large_payload < small_payload + 30
