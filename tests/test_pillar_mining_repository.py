from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from importlib import import_module
from io import StringIO

from alembic.migration import MigrationContext
from alembic.operations import Operations
import pytest
from sqlalchemy import create_engine, event, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError

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
    iter_unit_layers,
    validate_mining_run,
)


def _run(**overrides) -> MiningRunContract:
    summary = MiningUnitContract(
        unit_type="summary",
        unit_key="summary",
        producer_status="ACTIVE",
        canonical_status="SUCCESS",
        signal_axis="SIDE",
        payload={"P2_SIGNAL_PROFILE": {"FT": {"1X2": {"DIRECTION": "HOME"}}}},
    )
    module = MiningUnitContract(
        unit_type="module",
        unit_key="p2_signal_engine",
        parent_unit_key="summary",
        module_id="p2_signal_engine",
        producer_status="ACTIVE",
        canonical_status="SUCCESS",
        signal_axis="SIDE",
        payload={"P2_SIGNAL_PROFILE": {"FT": {"1X2": {"DIRECTION": "HOME"}}}},
    )
    values = {
        "event_id": 1,
        "pillar_id": "pillar_2_side_market",
        "result_scope": "side_market",
        "execution_slot": "evaluation:5",
        "engine_version": "p2-signal-profile-v1",
        "payload_schema_version": 2,
        "producer_status": "ACTIVE",
        "canonical_status": "SUCCESS",
        "sport": "Football",
        "evaluation_minute": 5,
        "target_minute": 5,
        "context": {"minutes_to_start": 5},
        "inputs": {"PIN_HOME": 2.0},
        "diagnostics": {"input_trace": {"PIN_HOME": {"quote_id": 1}}},
        "output_payload": {
            "P2_STATUS": "ACTIVE",
            "P2_SIGNAL_PROFILE": {"FT": {"1X2": {"DIRECTION": "HOME"}}},
        },
        "units": (summary, module),
        "calculated_at": datetime(2026, 8, 22, 17, 55, tzinfo=timezone.utc),
    }
    values.update(overrides)
    return MiningRunContract(**values)


def _event() -> Event:
    return Event(
        id=1,
        slug="home-away",
        starts_at=datetime(2026, 8, 22, 18, 0, tzinfo=timezone.utc),
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

    replacement_profile = {"FT": {"1X2": {"DIRECTION": "AWAY"}}}
    replacement_summary = replace(
        original.units[0], payload={"P2_SIGNAL_PROFILE": replacement_profile}
    )
    replacement_module = replace(
        original.units[1],
        payload={"P2_SIGNAL_PROFILE": replacement_profile},
    )
    PillarMiningRepository.replace_run(
        replace(original, units=(replacement_summary, replacement_module))
    )
    PillarMiningRepository.replace_run(
        replace(original, execution_slot="evaluation:0", evaluation_minute=0)
    )
    PillarMiningRepository.replace_run(
        replace(original, engine_version="p2-signal-profile-v2")
    )

    with manager.get_session() as session:
        assert session.query(PillarMiningRun).count() == 3
        canonical = (
            session.query(PillarMiningRun)
            .filter_by(
                execution_slot="evaluation:5",
                engine_version="p2-signal-profile-v1",
            )
            .one()
        )
        units = session.query(PillarMiningUnit).filter_by(run_id=canonical.id).all()
        assert len(units) == 2
        summary = next(unit for unit in units if unit.unit_type == "summary")
        module = next(unit for unit in units if unit.unit_type == "module")
        assert summary.score is None
        assert summary.direction is None
        assert module.parent_unit_id == summary.id
        assert (
            session.query(PillarMiningMetricValue).filter_by(unit_id=module.id).count()
            == 0
        )


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
        assert joined == (None, "1")
        session.delete(session.query(Event).filter_by(id=1).one())

    with manager.get_session() as session:
        assert session.query(PillarMiningRun).count() == 0
        assert session.query(PillarMiningUnit).count() == 0
        assert session.query(PillarMiningMetricValue).count() == 0


def test_postgresql_run_upsert_is_atomic() -> None:
    values = PillarMiningRepository._run_values(_run())
    statement = PillarMiningRepository._build_atomic_upsert(values, "postgresql")
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert (
        "ON CONFLICT (event_id, pillar_id, result_scope, execution_slot, engine_version)"
        in sql
    )
    assert "DO UPDATE SET" in sql


def test_reader_preserves_historical_status_and_payload(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch, "historical_reader.db")
    old = _run(producer_status="PARTIAL", canonical_status="PARTIAL")
    PillarMiningRepository.replace_run(old)
    with manager.get_session() as session:
        run_id = session.query(PillarMiningRun.id).scalar()
    stored = PillarMiningRepository.get_result(run_id)
    assert stored["result"] == old.output_payload
    assert stored["producer_status"] == stored["canonical_status"] == "PARTIAL"
    assert stored["payload_schema_version"] == 2


def test_reader_reconstructs_v4_signals_from_units_without_profile_copies(
    tmp_path, monkeypatch
):
    from modules.pillars.mining.adapters import P2MiningAdapter
    from tests.pillars.test_market_evaluation import evaluate, quotes, event
    from modules.pillars.mining.serialization import to_json_value

    manager = _manager(tmp_path, monkeypatch, "current_reader.db")
    from infrastructure.persistence.models import Bookie

    with manager.get_session() as session:
        session.add(Bookie(bookie_id=302, name="Pinnacle", slug="pinnacle"))
    identity = event()
    identity.event_id = 1
    result = evaluate(2, quotes())
    run = P2MiningAdapter().build(identity, result)
    PillarMiningRepository.replace_run(run)
    with manager.get_session() as session:
        model = session.query(PillarMiningRun).one()
        assert "signals" not in model.output_payload
        stored = PillarMiningRepository.get_result(model.id, session=session)
    assert stored["canonical_status"] == "SUCCESS"
    assert stored["result"]["inputs"] == to_json_value(result["inputs"])
    assert [s["status"] for s in stored["result"]["signals"]] == [
        s["status"] for s in result["signals"]
    ]
    assert [s["key"] for s in stored["result"]["signals"]] == [
        s["key"] for s in result["signals"]
    ]
    for original, loaded in zip(result["signals"], stored["result"]["signals"]):
        if isinstance(original["value"], (int, float)):
            assert abs(original["value"] - loaded["value"]) < 1e-10
        else:
            assert original["value"] == loaded["value"]


def _wide_run(count=1201):
    root = MiningUnitContract('summary', 'summary', 'ACTIVE', 'SUCCESS')
    leaves = tuple(
        MiningUnitContract(
            'signal', f'node_{i}', 'ACTIVE', 'SUCCESS',
            parent_unit_key='summary', ordinal=i,
            payload={'index': i}, diagnostics={'retained': True},
            metrics=(
                PillarMiningMetric('amount', 'number', Decimal(i) / 8),
                PillarMiningMetric('label', 'text', f'value_{i}'),
                PillarMiningMetric('flag', 'boolean', bool(i % 2)),
            ),
        ) for i in range(count)
    )
    grandchild = MiningUnitContract('component', 'grandchild', 'INSUFFICIENT_DATA', 'INSUFFICIENT',
                     parent_unit_key='node_5', diagnostics={'reason': 'MISSING_INPUT'})
    return _run(units=(grandchild, *reversed(leaves), root))


def test_wide_graph_batches_sql_and_maps_unordered_returning_rows(tmp_path, monkeypatch):
    db = _manager(tmp_path, monkeypatch, 'bulk.db')
    statements = []

    @event.listens_for(db.engine, 'before_cursor_execute')
    def count_sql(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    run = _wide_run()
    validate_mining_run(run)
    with db.get_session() as session:
        execute = session.execute

        def shuffled(statement, *args, **kwargs):
            result = execute(statement, *args, **kwargs)
            if getattr(statement, 'is_insert', False) and statement.table.name == 'pillar_mining_units':
                return reversed(result.all())
            return result

        monkeypatch.setattr(session, 'execute', shuffled)
        PillarMiningRepository.replace_run(run, session=session)
        assert not any(isinstance(obj, (PillarMiningUnit, PillarMiningMetricValue))
                       for obj in session.identity_map.values())
    # This guard detects a return to per-unit INSERT/flush regardless of hardware.
    assert len(statements) < 40
    assert not any(s.lstrip().startswith('SELECT') for s in statements)
    with db.get_session() as session:
        units = {unit.unit_key: unit for unit in session.query(PillarMiningUnit)}
        assert len(units) == 1203
        root_id = units['summary'].id
        for i in range(1201):
            unit = units[f'node_{i}']
            assert unit.parent_unit_id == root_id and unit.ordinal == i
            assert unit.payload == {'index': i} and unit.diagnostics == {'retained': True}
            values = {metric.metric_name: metric for metric in unit.metrics}
            assert values['amount'].numeric_value == Decimal(i) / 8
            assert values['label'].text_value == f'value_{i}'
            assert values['flag'].boolean_value is bool(i % 2)
        assert units['grandchild'].parent_unit_id == units['node_5'].id
        assert units['grandchild'].canonical_status == 'INSUFFICIENT'
    statements.clear()
    PillarMiningRepository.replace_run(_run())
    assert len(statements) < 15
    with db.get_session() as session:
        assert session.query(PillarMiningRun).count() == 1
        assert session.query(PillarMiningUnit).count() == 2
        assert session.query(PillarMiningMetricValue).count() == 0


def test_failed_later_batch_rolls_back_header_deletes_and_earlier_batches(tmp_path, monkeypatch):
    db = _manager(tmp_path, monkeypatch, 'bulk_rollback.db')
    original = _run()
    PillarMiningRepository.replace_run(original)
    run = _wide_run()
    # node_0 is in the last sibling batch because declarations are reversed.
    units = tuple(replace(unit, metrics=unit.metrics + unit.metrics[:1])
                  if unit.unit_key == 'node_0' else unit for unit in run.units)
    with pytest.raises(IntegrityError):
        PillarMiningRepository.replace_run(replace(run, inputs={'changed': True}, units=units))
    with db.get_session() as session:
        assert session.query(PillarMiningRun).one().inputs == original.inputs
        assert {key for key, in session.query(PillarMiningUnit.unit_key)} == {'summary', 'p2_signal_engine'}
        assert session.query(PillarMiningMetricValue).count() == 0


def test_supplied_transaction_can_rollback_entire_replacement(tmp_path, monkeypatch):
    db = _manager(tmp_path, monkeypatch, 'caller_transaction.db')
    with db.SessionLocal() as session:
        PillarMiningRepository.replace_run(_run(), session=session)
        assert session.scalar(select(PillarMiningRun.id)) is not None
        session.rollback()
    with db.get_session() as session:
        assert session.query(PillarMiningRun).count() == 0
        assert session.query(PillarMiningUnit).count() == 0


@pytest.mark.parametrize('units,reason', [
    ((MiningUnitContract('x', 'x', 'ACTIVE', 'SUCCESS', parent_unit_key='absent'),), 'unknown parent'),
    ((MiningUnitContract('x', 'x', 'ACTIVE', 'SUCCESS', parent_unit_key='y'),
      MiningUnitContract('x', 'y', 'ACTIVE', 'SUCCESS', parent_unit_key='x')), 'cycle'),
    ((MiningUnitContract('x', 'x', 'ACTIVE', 'SUCCESS'), MiningUnitContract('x', 'x', 'ACTIVE', 'SUCCESS')), 'duplicate'),
])
def test_invalid_graph_is_rejected(units, reason):
    with pytest.raises(ValueError, match=reason):
        validate_mining_run(_run(units=units))


def test_deep_graph_does_not_require_recursion():
    units = tuple(MiningUnitContract('component', str(i), 'ACTIVE', 'SUCCESS',
                       parent_unit_key=str(i - 1) if i else None) for i in range(2000))
    validate_mining_run(_run(units=tuple(reversed(units))))
    assert [unit.unit_key for layer in iter_unit_layers(units) for unit in layer] == [str(i) for i in range(2000)]


def _migration(monkeypatch, context):
    module = import_module('infrastructure.persistence.alembic.versions.20261004_01_mining_parent_index')
    monkeypatch.setattr(module, 'op', Operations(context))
    return module


def test_index_upgrade_is_repeatable_and_preserves_units(monkeypatch):
    engine = create_engine('sqlite://')
    with engine.begin() as connection:
        connection.execute(text('CREATE TABLE pillar_mining_units(id INTEGER PRIMARY KEY, parent_unit_id INTEGER)'))
        connection.execute(text('INSERT INTO pillar_mining_units VALUES (1, NULL), (2, 1)'))
        module = _migration(monkeypatch, MigrationContext.configure(connection))
        module.upgrade()
        module.upgrade()
        plan = connection.execute(text('EXPLAIN QUERY PLAN SELECT id FROM pillar_mining_units WHERE parent_unit_id=1')).all()
        assert 'USING COVERING INDEX idx_pillar_mining_unit_parent' in str(plan)
        module.downgrade()
        assert inspect(connection).get_indexes('pillar_mining_units') == []
        assert connection.execute(text('SELECT * FROM pillar_mining_units ORDER BY id')).all() == [(1, None), (2, 1)]


def test_postgresql_index_is_built_concurrently(monkeypatch):
    output = StringIO()
    module = _migration(monkeypatch, MigrationContext.configure(
        dialect_name='postgresql', opts={'as_sql': True, 'output_buffer': output},
    ))
    module.upgrade()
    module.downgrade()
    ddl = output.getvalue()
    assert 'CREATE INDEX CONCURRENTLY idx_pillar_mining_unit_parent' in ddl
    assert 'DROP INDEX CONCURRENTLY IF EXISTS idx_pillar_mining_unit_parent' in ddl
    assert 'ALTER TABLE' not in ddl
