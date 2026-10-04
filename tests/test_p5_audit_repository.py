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
from tests.test_pillar_mining_repository import _event


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
        PillarMiningRepository.replace_run(run, session=session)
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


def test_replacement_cascades_only_owned_sample(manager):
    first = persist(manager)
    second = persist(manager)
    with manager.get_session() as session:
        assert session.get(P5MemorySample, first) is None
        assert session.get(P5MemorySample, second).sample_size == 4
        assert (
            session.query(P5MemorySampleMember).filter_by(sample_id=first).count() == 0
        )


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


def test_replacement_rollback_keeps_previous_complete_execution(manager):
    previous = persist(manager)
    with pytest.raises(RuntimeError):
        with manager.get_session() as session:
            result = evaluate(
                5, quotes(), Pillar5PriceMemoryRepository(session=session, capture=True)
            )
            PillarMiningRepository.replace_run(
                P5MiningAdapter().build(event(), result), session=session
            )
            session.flush()
            raise RuntimeError("transaction failed after replacement")
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    assert repository.get_sample_page(previous)["sample_size"] == 4
    with manager.get_session() as session:
        assert session.query(P5MemorySample).count() == 1
        assert session.query(P5MemorySampleMember).count() == 4


def test_replacement_keeps_samples_of_other_engine_versions(manager):
    with manager.get_session() as session:
        result = evaluate(
            5, quotes(), Pillar5PriceMemoryRepository(session=session, capture=True)
        )
        original = next(p["sample_id"] for p in result["analysis"].values())
        run = P5MiningAdapter().build(event(), result)
        PillarMiningRepository.replace_run(
            replace(run, engine_version="p5-preserved-version"), session=session
        )
    current = persist(manager)
    persist(manager)
    repository = Pillar5PriceMemoryRepository(manager.SessionLocal)
    assert (
        repository.get_sample_page(original)["engine_version"] == "p5-preserved-version"
    )
    with pytest.raises(LookupError):
        repository.get_sample_page(current)
    with manager.get_session() as session:
        assert session.query(P5MemorySample).count() == 2


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
    assert page["payload_schema_version"] == 4
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


def test_pipeline_uses_one_transaction_for_sample_and_result(manager, monkeypatch):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from modules.pillars.mining.service import PillarMiningService
    from tests.pillars.test_structural_target_selection import _event_context

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    context = _event_context()
    context.event_id, context.starts_at, context.odds_trajectory = 900, START, quotes()
    service = PillarMiningService(
        PillarMiningRepository, {"pillar_5": P5MiningAdapter()}
    )
    processor = pipeline.EventPillarProcessor(
        None,
        enabled_pillars={
            "pillar_1": False,
            "pillar_2": False,
            "pillar_3": False,
            "pillar_4": False,
            "pillar_5": True,
        },
        mining_service=service,
    )
    output = processor.process_event(context)
    assert output["pillar_5"]["status"] == "ACTIVE"
    sample_id = next(p["sample_id"] for p in output["pillar_5"]["analysis"].values())
    assert (
        Pillar5PriceMemoryRepository(manager.SessionLocal).get_sample_page(sample_id)[
            "run_id"
        ]
        is not None
    )
    assert context.odds_trajectory == []


@pytest.mark.parametrize("has_history", [True, False])
def test_pipeline_rolls_back_audit_if_writer_fails(manager, monkeypatch, has_history):
    import modules.jobs.pre_start_check_job.pillar_pipeline as pipeline
    from modules.pillars.mining.service import PillarMiningService
    from tests.pillars.test_structural_target_selection import _event_context

    monkeypatch.setattr(pipeline, "db_manager", manager)
    monkeypatch.setattr(pipeline, "_is_pillar_competition_in_scope", lambda _: True)
    if not has_history:
        with manager.get_session() as session:
            session.execute(text("DELETE FROM mv_p5_price_memory"))

    class FailingWriter:
        def replace_run(self, run, *, session):
            PillarMiningRepository.replace_run(run, session=session)
            raise RuntimeError("write failed")

    context = _event_context()
    context.event_id, context.starts_at, context.odds_trajectory = 900, START, quotes()
    service = PillarMiningService(FailingWriter(), {"pillar_5": P5MiningAdapter()})
    processor = pipeline.EventPillarProcessor(
        None,
        enabled_pillars={
            "pillar_1": False,
            "pillar_2": False,
            "pillar_3": False,
            "pillar_4": False,
            "pillar_5": True,
        },
        mining_service=service,
    )
    result = processor.process_event(context)["pillar_5"]
    assert result["status"] == ("ACTIVE" if has_history else "ERROR")
    assert result["execution_status"] == (
        "COMPLETED_WITH_ERRORS" if has_history else "FAILED"
    )
    assert all(p.get("sample_id") is None for p in result["analysis"].values())
    with manager.get_session() as session:
        assert session.query(P5MemorySample).count() == 0
        assert session.query(P5MemorySampleMember).count() == 0
        assert session.query(PillarMiningRun).count() == 0
