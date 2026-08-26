from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from sqlalchemy import inspect, text
from sqlalchemy.dialects import postgresql

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Event,
    PillarMiningMetricValue,
    PillarMiningRun,
    PillarMiningUnit,
    Result,
)
from infrastructure.persistence.repositories import pillar_mining_repository
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository,
)
from modules.pillars.mining.contracts import (
    PillarMiningMetric,
    PillarMiningRun as MiningRunContract,
    PillarMiningUnit as MiningUnitContract,
)


def _run(**overrides) -> MiningRunContract:
    summary = MiningUnitContract(
        unit_type="summary",
        unit_key="summary",
        producer_status="ACTIVE",
        canonical_status="SUCCESS",
        signal_axis="SIDE",
        score_name="SIDE_MARKET_EDGE",
        score=Decimal("0.10"),
        direction="HOME",
    )
    module = MiningUnitContract(
        unit_type="module",
        unit_key="p2_raw_engine",
        parent_unit_key="summary",
        module_id="p2_raw_engine",
        producer_status="ACTIVE",
        canonical_status="SUCCESS",
        signal_axis="SIDE",
        score_name="SIDE_MARKET_EDGE",
        score=Decimal("0.10"),
        direction="HOME",
        metrics=(
            PillarMiningMetric(
                name="SIDE_MARKET_EDGE",
                value_type="number",
                value=Decimal("0.10"),
                group="result",
            ),
            PillarMiningMetric(
                name="P2_DIRECTION_RAW",
                value_type="text",
                value="HOME",
                group="result",
            ),
        ),
    )
    values = {
        "event_id": 1,
        "pillar_id": "pillar_2_side_market",
        "result_scope": "side_market",
        "execution_slot": "evaluation:5",
        "engine_version": "p2_raw_v1",
        "payload_schema_version": 2,
        "producer_status": "ACTIVE",
        "canonical_status": "SUCCESS",
        "sport": "Football",
        "evaluation_minute": 5,
        "target_minute": 5,
        "context": {"minutes_to_start": 5},
        "inputs": {"PIN_HOME": 2.0},
        "diagnostics": {"input_trace": {"PIN_HOME": {"quote_id": 1}}},
        "output_payload": {"P2_STATUS": "ACTIVE"},
        "units": (summary, module),
        "calculated_at": datetime(2026, 8, 22, 17, 55),
    }
    values.update(overrides)
    return MiningRunContract(**values)


def _event() -> Event:
    return Event(
        id=1,
        slug="home-away",
        start_time_utc=datetime(2026, 8, 22, 18, 0),
        sport="Football",
        competition="League",
        home_team="Home",
        away_team="Away",
        gender="Men",
        discovery_source="test",
        round="regular_season",
    )


def _manager(tmp_path, monkeypatch, name: str) -> DatabaseManager:
    manager = DatabaseManager(f"sqlite:///{tmp_path / name}")
    manager.create_tables()
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)
    with manager.get_session() as session:
        session.execute(text("PRAGMA foreign_keys = ON"))
        session.add(_event())
    return manager


def test_repository_replaces_graph_and_keeps_other_slots_and_versions(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch, "mining.db")
    original = _run()
    PillarMiningRepository.replace_run(original)

    replacement_summary = replace(
        original.units[0], score=Decimal("0.25"), direction="AWAY"
    )
    replacement_module = replace(
        original.units[1],
        score=Decimal("0.25"),
        direction="AWAY",
        metrics=(
            PillarMiningMetric(
                name="SIDE_MARKET_EDGE",
                value_type="number",
                value=Decimal("0.25"),
            ),
        ),
    )
    PillarMiningRepository.replace_run(
        replace(original, units=(replacement_summary, replacement_module))
    )
    PillarMiningRepository.replace_run(
        replace(original, execution_slot="evaluation:0", evaluation_minute=0)
    )
    PillarMiningRepository.replace_run(
        replace(original, engine_version="p2_raw_v2")
    )

    with manager.get_session() as session:
        assert session.query(PillarMiningRun).count() == 3
        canonical = (
            session.query(PillarMiningRun)
            .filter_by(execution_slot="evaluation:5", engine_version="p2_raw_v1")
            .one()
        )
        units = session.query(PillarMiningUnit).filter_by(run_id=canonical.id).all()
        assert len(units) == 2
        summary = next(unit for unit in units if unit.unit_type == "summary")
        module = next(unit for unit in units if unit.unit_type == "module")
        assert summary.score == Decimal("0.250000000000")
        assert summary.direction == "AWAY"
        assert module.parent_unit_id == summary.id
        metrics = session.query(PillarMiningMetricValue).filter_by(unit_id=module.id).all()
        assert [(metric.metric_name, metric.numeric_value) for metric in metrics] == [
            ("SIDE_MARKET_EDGE", Decimal("0.250000000000"))
        ]


def test_run_joins_results_and_event_delete_cascades_graph(
    tmp_path,
    monkeypatch,
) -> None:
    manager = _manager(tmp_path, monkeypatch, "mining_join.db")
    with manager.get_session() as session:
        session.add(Result(event_id=1, home_score=2, away_score=0, winner="1"))

    PillarMiningRepository.replace_run(_run())

    with manager.get_session() as session:
        joined = (
            session.query(PillarMiningUnit.direction, Result.winner)
            .join(PillarMiningRun, PillarMiningRun.id == PillarMiningUnit.run_id)
            .join(Result, Result.event_id == PillarMiningRun.event_id)
            .filter(PillarMiningUnit.unit_type == "summary")
            .one()
        )
        assert joined == ("HOME", "1")
        session.delete(session.query(Event).filter_by(id=1).one())

    with manager.get_session() as session:
        assert session.query(PillarMiningRun).count() == 0
        assert session.query(PillarMiningUnit).count() == 0
        assert session.query(PillarMiningMetricValue).count() == 0


def test_schema_migration_drops_experiment_and_is_idempotent(tmp_path) -> None:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'mining_migration.db'}")
    with manager.engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE pillar_mining_observations (id INTEGER PRIMARY KEY)")
        )

    manager._migrate_pillar_mining_schema_v2()
    manager._migrate_pillar_mining_schema_v2()

    table_names = set(inspect(manager.engine).get_table_names())
    assert "pillar_mining_observations" not in table_names
    assert {
        "pillar_mining_runs",
        "pillar_mining_units",
        "pillar_mining_metric_values",
    } <= table_names


def test_postgresql_run_upsert_is_atomic() -> None:
    values = PillarMiningRepository._run_values(_run())
    statement = PillarMiningRepository._build_atomic_upsert(values, "postgresql")
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT (event_id, pillar_id, result_scope, execution_slot, engine_version)" in sql
    assert "DO UPDATE SET" in sql
