"""Real SQL acceptance: frozen populations, paging, ownership and rollback."""

from datetime import timedelta
from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import text

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    P5MemorySample,
    P5MemorySampleMember,
    Bookie,
    Competition,
    PillarMiningRun,
)
from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    Pillar5PriceMemoryRepository,
)
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository,
)
from modules.pillars.mining.adapters import P5MiningAdapter
from modules.pillars.pillar_5.calculation_models import (
    MemoryQueryKey,
    PopulationFilters,
)
from tests.pillars.test_market_evaluation import START, event, evaluate, quotes
from tests.test_pillar_mining_repository import _event, snapshot


@pytest.fixture
def manager(tmp_path):
    db = DatabaseManager(f"sqlite:///{tmp_path / 'audit.db'}")
    db.create_tables()
    with db.get_session() as session:
        session.execute(text("PRAGMA foreign_keys=ON"))
        owner = _event()
        owner.id = 900
        session.add(owner)
        session.add(Bookie(bookie_id=302, name="Pinnacle", slug="pinnacle"))
        session.add(
            Competition(
                competition_id=99,
                source="test",
                source_tournament_id=99,
                canonical_name="League",
                display_name="League",
            )
        )
        session.execute(text("""CREATE TABLE mv_p5_price_memory (
            event_id INTEGER, sport TEXT, competition_id INTEGER, season_id INTEGER, country TEXT,
            bookie_id INTEGER, market_group TEXT, market_period TEXT, has_draw BOOLEAN, starts_at TIMESTAMP,
            odds_home NUMERIC, odds_draw NUMERIC, odds_away NUMERIC, home_score INTEGER, away_score INTEGER,
            winner_side TEXT, last_sync_at TIMESTAMP)"""))
        for i, winner in enumerate(("HOME", "1", "2", "X", "INVALID", "1"), 1):
            session.execute(
                text(
                    """INSERT INTO mv_p5_price_memory VALUES
                (:id,'Football',11,2026,'Mexico',302,'1X2','Full Time',1,:date,2,2,2,1,0,:winner,:date)"""
                ),
                {
                    "id": i if i != 6 else 1,
                    "date": START - timedelta(days=i),
                    "winner": winner,
                },
            )
        for eid, delta in ((900, -1), (700, 1)):
            session.execute(
                text(
                    """INSERT INTO mv_p5_price_memory VALUES
                (:id,'Football',11,2026,'Mexico',302,'1X2','Full Time',1,:date,2,2,2,1,0,'1',:date)"""
                ),
                {"id": eid, "date": START + timedelta(days=delta)},
            )
    yield db
    db.engine.dispose()


def key():
    return MemoryQueryKey(
        "Football",
        302,
        "1X2",
        "Full Time",
        "THREE_WAY",
        Decimal("2"),
        Decimal("2"),
        Decimal("2"),
    )


def test_summary_is_exhaustive_causal_deduplicated_and_filterable(manager):
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    sample = repository.summarize(
        key=key(),
        current_event_id=900,
        current_starts_at=START,
        population_filters=PopulationFilters(),
    )
    assert (
        sample.sample_size,
        sample.wins_home,
        sample.wins_draw,
        sample.wins_away,
    ) == (4, 2, 1, 1)
    assert sample.sample_id is None
    empty = repository.summarize(
        key=key(),
        current_event_id=900,
        current_starts_at=START,
        population_filters=PopulationFilters(country="Spain"),
    )
    assert empty.sample_size == 0


def persist(manager):
    with manager.get_session() as session:
        result = evaluate(
            5, quotes(), Pillar5PriceMemoryRepository(session=session, capture=True)
        )
        run = P5MiningAdapter().build(event(), result)
        original = snapshot(event_id=900)
        PillarMiningRepository.append_evaluation(original, session=session)
        PillarMiningRepository.append_run(
            replace(run, evaluation_id=original.evaluation_id), session=session
        )
        return next(p["sample_id"] for p in result["analysis"].values())


def test_frozen_detail_survives_source_mutation_and_pages_exactly(manager):
    sample_id = persist(manager)
    with manager.get_session() as session:
        session.execute(text("DELETE FROM mv_p5_price_memory"))
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    first = repository.get_sample_page(sample_id, page_size=2)
    second = repository.get_sample_page(
        sample_id, cursor=first["next_cursor"], page_size=2
    )
    assert [row["event_id"] for row in first["items"] + second["items"]] == [1, 2, 3, 4]
    assert second["next_cursor"] is None
    assert first["sample_size"] == 4
    assert first["items"][0]["winner_side"] == "HOME"
    with pytest.raises(ValueError):
        repository.get_sample_page(sample_id, page_size=1001)


def test_transaction_failure_does_not_leave_sample_or_partial_result(manager):
    with pytest.raises(RuntimeError):
        with manager.get_session() as session:
            evaluate(
                5, quotes(), Pillar5PriceMemoryRepository(session=session, capture=True)
            )
            raise RuntimeError("result write failed")
    with manager.get_session() as session:
        assert session.query(P5MemorySample).count() == 0
        assert session.query(P5MemorySampleMember).count() == 0


def test_tied_start_times_use_event_id_as_cursor_tiebreaker(manager):
    with manager.get_session() as session:
        session.execute(
            text(
                "UPDATE mv_p5_price_memory SET starts_at = (SELECT starts_at FROM mv_p5_price_memory WHERE event_id=2 LIMIT 1) WHERE event_id IN (1,3,4)"
            )
        )
    sample_id = persist(manager)
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    cursor, ids = None, []
    while True:
        page = repository.get_sample_page(sample_id, cursor, 1)
        ids += [row["event_id"] for row in page["items"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert ids == [4, 3, 2, 1]
    page = repository.get_sample_page(sample_id)
    assert page["counts"] == {"home": 2, "draw": 1, "away": 1}
    assert page["payload_schema_version"] == 5
    assert page["cutoff"] == START.isoformat()
    for invalid in (0, True, "100", 1001):
        with pytest.raises(ValueError):
            repository.get_sample_page(sample_id, page_size=invalid)


def test_captured_sample_is_owned_even_when_calculation_fails(manager, monkeypatch):
    import importlib

    run_module = importlib.import_module("modules.pillars.pillar_5.run_pillar_5")

    def fail(*args, **kwargs):
        raise ArithmeticError("formula failed")

    monkeypatch.setattr(run_module, "calculate_memory_profile", fail)
    sample_id = persist(manager)
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    page = repository.get_sample_page(sample_id)
    with manager.get_session() as session:
        result = PillarMiningRepository.get_result(page["run_id"], session=session)
    assert result["result"]["status"] == "ERROR"
    assert page["sample_size"] == 4
    assert any(s["reason"] == "CALCULATION_ERROR" for s in result["result"]["signals"])


def test_new_evaluation_keeps_previous_sample(manager):
    first, second = persist(manager), persist(manager)
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    assert first != second
    assert repository.get_sample_page(first)["sample_size"] == 4
    assert repository.get_sample_page(second)["sample_size"] == 4


@pytest.mark.parametrize("table", ["p5_memory_samples", "p5_memory_sample_members"])
def test_bound_population_cannot_be_modified(manager, table):
    from sqlalchemy.exc import IntegrityError

    persist(manager)
    with pytest.raises(IntegrityError, match="immutable"):
        with manager.get_session() as session:
            session.execute(text(f"DELETE FROM {table}"))


def pipeline_context():
    from tests.pillars.test_structural_target_selection import _event_context

    context = _event_context()
    context.event_id, context.starts_at, context.odds_trajectory = 900, START, quotes()
    return context


def test_pipeline_freezes_six_scopes_and_sample_before_cutoff(manager, monkeypatch):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from infrastructure.persistence.repositories import pillar_mining_repository
    from infrastructure.persistence.repositories.pillar_mining_repository import (
        PillarMiningRepository,
    )
    from datetime import datetime

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    context = pipeline_context()
    processor = pipeline.EventPillarProcessor(enabled_pillars={"pillar_1": False})
    output = processor.process_event(context)
    stored = PillarMiningRepository.get_evaluation(output["evaluation_id"])
    assert len(stored["pillars"]) == 6
    assert output["pillar_5"]["status"] == "ACTIVE"
    assert {
        p["status"]
        for p in stored["pillars"]
        if p["pillar_id"] == "pillar_1_team_structure"
    } == {"SKIPPED"}
    sample_id = next(p["sample_id"] for p in output["pillar_5"]["analysis"].values())
    page = Pillar5PriceMemoryRepository(manager.SessionLocal).get_sample_page(sample_id)
    assert datetime.fromisoformat(page["acquired_at"]) <= output["evaluation_as_of"]
    assert (
        stored["inputs"]["odds_observations"][0]["collected_at"]
        <= stored["evaluation_as_of"]
    )
    with manager.get_session() as session:
        session.execute(text("DELETE FROM mv_p5_price_memory"))
    assert PillarMiningRepository.get_evaluation(output["evaluation_id"]) == stored


def test_pipeline_storage_failure_propagates_and_rolls_back_everything(
    manager, monkeypatch
):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from modules.pillars.mining.service import PillarMiningService
    from infrastructure.persistence.models import PillarEvaluation

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)

    class FailingWriter(PillarMiningRepository):
        @classmethod
        def append_run(cls, run, *, session):
            value = super().append_run(run, session=session)
            if run.pillar_id == "pillar_5":
                raise RuntimeError("write failed")
            return value

    processor = pipeline.EventPillarProcessor(
        enabled_pillars={"pillar_1": False},
        mining_service=PillarMiningService(
            FailingWriter(), pipeline._registered_mining_adapters()
        ),
    )
    with pytest.raises(RuntimeError, match="write failed"):
        processor.process_event(pipeline_context())
    with manager.get_session() as session:
        assert session.query(PillarEvaluation).count() == 0
        assert session.query(PillarMiningRun).count() == 0
        assert session.query(P5MemorySample).count() == 0


def test_sample_capture_failure_propagates_before_scoring(manager, monkeypatch):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from infrastructure.persistence.models import PillarEvaluation
    from modules.pillars.pillar_5.ports import MemorySamplePersistenceError

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    with manager.get_session() as session:
        session.execute(text("DROP TABLE p5_memory_sample_members"))

    processor = pipeline.EventPillarProcessor(enabled_pillars={"pillar_1": False})
    with pytest.raises(MemorySamplePersistenceError, match="sample capture failed"):
        processor.process_event(pipeline_context())

    with manager.get_session() as session:
        assert session.query(PillarEvaluation).count() == 0
        assert session.query(PillarMiningRun).count() == 0
        assert session.query(P5MemorySample).count() == 0


def test_p1_missing_inputs_and_early_abort_are_preserved(manager, monkeypatch):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from infrastructure.persistence.repositories import pillar_mining_repository
    from modules.pillars.instrumentation import record_calculation

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    monkeypatch.setattr(
        pipeline,
        "resolve_matchup_streak_analysis",
        lambda *args, **kwargs: (None, False),
    )
    processor = pipeline.EventPillarProcessor(enabled_pillars={"pillar_5": False})
    first = processor.process_event(pipeline_context())
    assert first["pillar_1"]["reason"] == "MISSING_INPUT"
    context = pipeline_context()
    monkeypatch.setattr(
        pipeline, "resolve_matchup_streak_analysis", lambda *args, **kwargs: ({}, False)
    )

    def abort(*args, **kwargs):
        record_calculation(
            "totals_abort", reason="ABORT_N_AVAIL_ZERO", evidence={"available": 0}
        )

    monkeypatch.setattr(pipeline, "calculate_p1_totals", abort)
    monkeypatch.setattr(
        pipeline,
        "calculate_p1_side",
        lambda *args, **kwargs: {
            "modules": [],
            "status": "ERROR",
            "reason": "CALCULATION_ERROR",
        },
    )
    output = processor.process_event(context)
    assert output["pillar_1_totals"]["reason"] == "INSUFFICIENT_OBSERVATIONS"
    stored = PillarMiningRepository.get_evaluation(output["evaluation_id"])
    totals = next(p for p in stored["pillars"] if p["result_scope"] == "totals")
    assert totals["result"]["evidence"]["native_reason"] == "ABORT_N_AVAIL_ZERO"
    assert totals["result"]["diagnostics"]["signals"]["blocked_signals"] == 1


def test_pipeline_preserves_existing_inputs_without_id_exclusions(manager, monkeypatch):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from infrastructure.persistence.repositories import pillar_mining_repository

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    context = pipeline_context()
    context.odds_trajectory[0]["snapshot_id"] = 0
    output = pipeline.EventPillarProcessor(
        enabled_pillars={"pillar_1": False}
    ).process_event(context)
    stored = PillarMiningRepository.get_evaluation(output["evaluation_id"])
    assert len(stored["inputs"]["odds_observations"]) == 3
    assert stored["inputs"]["odds_observations"][0]["snapshot_id"] == 0
    for number in range(2, 6):
        assert all(
            s["key"] != "temporal_availability"
            for s in output[f"pillar_{number}"]["signals"]
        )


def test_persisted_p4_reuses_one_tick_with_distinct_checkpoint_times(
    manager, monkeypatch
):
    from datetime import timedelta
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from infrastructure.persistence.repositories import pillar_mining_repository

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    context = pipeline_context()
    for row in context.odds_trajectory:
        row["source_collected_at"] = START - timedelta(minutes=36)
    result = pipeline.EventPillarProcessor(
        enabled_pillars={"pillar_1": False}
    ).process_event(context)
    stored = PillarMiningRepository.get_evaluation(result["evaluation_id"])
    p4 = next(
        p
        for p in stored["pillars"]
        if p["pillar_id"] == "pillar_4_temporal_market_drift"
    )
    inputs = p4["result"]["inputs"]
    checkpoint_points = [
        p for p in inputs.values() if p.get("time_basis") == "CHECKPOINT_TARGET"
    ]
    assert any(
        p["target_minute"] == 30
        and p["effective_at"] == (START - timedelta(minutes=30)).isoformat()
        for p in checkpoint_points
    )
    assert any(
        p["target_minute"] == 5
        and p["effective_at"] == (START - timedelta(minutes=5)).isoformat()
        for p in checkpoint_points
    )
    source = inputs[checkpoint_points[0]["observation_ref"]]
    assert source["SOURCE_COLLECTED_AT"] == (START - timedelta(minutes=36)).isoformat()
    assert source["COLLECTED_AT"] == (START - timedelta(minutes=5)).isoformat()
    assert len([key for key in inputs if key.startswith("snapshot:")]) == 3
    with manager.get_session() as session:
        session.execute(text("DELETE FROM mv_p5_price_memory"))
    assert PillarMiningRepository.get_evaluation(result["evaluation_id"]) == stored
