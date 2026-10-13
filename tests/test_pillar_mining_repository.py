"""Immutable mining acceptance against isolated SQL databases."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4
from importlib import import_module
from io import StringIO
import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from alembic.migration import MigrationContext
from alembic.operations import Operations
from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    PillarEvaluation,
    PillarMiningRun,
    PillarMiningUnit,
    PillarMiningMetricValue,
    Event,
)
from infrastructure.persistence.repositories import (
    pillar_mining_repository as repository_module,
)
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository as Repository,
)
from infrastructure.persistence.repositories.outcome_observation_repository import (
    append_outcome_observation,
)
from modules.pillars.mining.contracts import (
    EvaluationSnapshot,
    PillarMiningRun as Run,
    PillarMiningUnit as Unit,
    PillarMiningMetric as Metric,
)
from modules.pillars.evaluation_contracts import SignalResult, EvaluationResult

NOW = datetime(2026, 10, 12, 12, tzinfo=timezone.utc)


def snapshot(evaluation_id=None, event_id=1):
    return EvaluationSnapshot(
        evaluation_id or str(uuid4()),
        event_id,
        NOW,
        context={"sport": "Football"},
        inputs={"original": Decimal("1.12345678901234567890123456789")},
        configuration={"threshold": ".15"},
        versions={"code_sha256": "effective-code", "commit_sha": "base-commit"},
    )


def _run(evaluation_id, **overrides):
    unit = Unit(
        "signal",
        "reading",
        "BLOCKED",
        parent_unit_key="summary",
        diagnostics={
            "reason": "INSUFFICIENT_OBSERVATIONS",
            "evidence": {"available": 1, "required": 2},
        },
        metrics=(
            Metric("composite", "number", Decimal("-.12345678901234567890123456789")),
        ),
    )
    fields = dict(
        event_id=1,
        evaluation_id=evaluation_id,
        pillar_id="pillar_2_side_market",
        result_scope="side_market",
        engine_version="test-v1",
        payload_schema_version=5,
        status="INSUFFICIENT_DATA",
        sport="Football",
        diagnostics={"signals": {"computed_signals": 0, "blocked_signals": 1}},
        units=(Unit("summary", "summary", "INSUFFICIENT_DATA"), unit),
    )
    fields.update(overrides)
    return Run(**fields)


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


@pytest.fixture
def manager(tmp_path, monkeypatch):
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'mining.db'}")
    manager.create_tables()
    monkeypatch.setattr(repository_module, "db_manager", manager)
    yield manager
    manager.engine.dispose()


def test_immutable_idempotent_and_exact(manager):
    original = snapshot()
    run = _run(original.evaluation_id)
    with manager.get_session() as session:
        Repository.append_evaluation(original, session=session)
        first = Repository.append_run(run, session=session)
    with manager.get_session() as session:
        Repository.append_evaluation(original, session=session)
        assert Repository.append_run(run, session=session) == first
        assert session.scalar(select(PillarMiningMetricValue.numeric_value)) == Decimal(
            "-.12345678901234567890123456789"
        )
    with pytest.raises(ValueError, match="conflicts"):
        with manager.get_session() as session:
            Repository.append_run(
                replace(run, inputs={"changed": True}), session=session
            )
    with pytest.raises(ValueError, match="conflicts"):
        with manager.get_session() as session:
            Repository.append_evaluation(replace(original, inputs={}), session=session)
    with manager.get_session() as session:
        assert session.query(PillarMiningRun).count() == 1
        assert session.query(PillarMiningUnit).count() == 2
        stored = Repository.get_evaluation(original.evaluation_id, session=session)
        assert stored["inputs"]["original"] == "1.12345678901234567890123456789"
        assert (
            stored["pillars"][0]["units"]["items"][1]["reason"]
            == "INSUFFICIENT_OBSERVATIONS"
        )


def test_operational_deletion_and_later_corrections_cannot_change_snapshot(manager):
    original = snapshot()
    with manager.get_session() as session:
        session.add(_event())
        Repository.append_evaluation(original, session=session)
        Repository.append_run(_run(original.evaluation_id), session=session)
    before = Repository.get_evaluation(original.evaluation_id)
    with manager.get_session() as session:
        session.execute(text("DELETE FROM events WHERE id=1"))
        append_outcome_observation(session, 1, {"home_score": 1, "away_score": 1})
        append_outcome_observation(session, 1, {"home_score": 2, "away_score": 1})
    case = Repository.get_mining_case(original.evaluation_id)
    assert case["evaluation"] == before
    assert case["outcome"]["payload"]["home_score"] == 2
    with manager.get_session() as session:
        assert (
            session.execute(
                text("SELECT COUNT(*) FROM event_outcome_observations")
            ).scalar()
            == 2
        )


@pytest.mark.parametrize(
    "table",
    [
        "pillar_evaluations",
        "pillar_mining_runs",
        "pillar_mining_units",
        "pillar_mining_metric_values",
        "event_outcome_observations",
    ],
)
def test_database_rejects_mutation(manager, table):
    original = snapshot()
    with manager.get_session() as session:
        Repository.append_evaluation(original, session=session)
        Repository.append_run(_run(original.evaluation_id), session=session)
        append_outcome_observation(session, 1, {"home_score": 1})
    with pytest.raises(IntegrityError, match="immutable"):
        with manager.get_session() as session:
            session.execute(text(f"DELETE FROM {table}"))


def test_rollback_and_missing_scope(manager):
    original = snapshot()
    with pytest.raises(ValueError, match="six"):
        with manager.get_session() as session:
            Repository.append_evaluation(original, session=session)
            Repository.append_run(_run(original.evaluation_id), session=session)
            Repository.require_complete_evaluation(
                original.evaluation_id, session=session
            )
    with manager.get_session() as session:
        assert session.query(PillarEvaluation).count() == 0
        assert session.query(PillarMiningUnit).count() == 0


def test_new_evaluation_same_minute_gets_new_identity(manager):
    for _ in range(2):
        with manager.get_session() as session:
            original = snapshot()
            Repository.append_evaluation(original, session=session)
            Repository.append_run(_run(original.evaluation_id), session=session)
    with manager.get_session() as session:
        assert session.query(PillarEvaluation).count() == 2
        assert session.query(PillarMiningRun).count() == 2


def test_reasons_and_aggregate_diagnostics():
    with pytest.raises(ValueError, match="registered reason"):
        SignalResult("missing", "BLOCKED")
    with pytest.raises(ValueError, match="registered reason"):
        SignalResult("missing", "BLOCKED", reason="ambiguous string")
    result = EvaluationResult(
        1,
        "p2",
        "v1",
        5,
        {},
        (
            SignalResult("a", "BLOCKED", reason="MISSING_INPUT"),
            SignalResult("b", "BLOCKED", reason="INSUFFICIENT_OBSERVATIONS"),
        ),
        (),
    ).to_dict()
    assert result["status"] == "INSUFFICIENT_DATA"
    assert result["diagnostics"]["signals"] == {
        "total_signals": 2,
        "computed_signals": 0,
        "blocked_signals": 2,
        "error_signals": 0,
        "reasons": {"MISSING_INPUT": 1, "INSUFFICIENT_OBSERVATIONS": 1},
    }


def test_batching_and_paged_units(manager, monkeypatch):
    original = snapshot()
    monkeypatch.setattr(repository_module, "UNIT_BATCH_SIZE", 3)
    monkeypatch.setattr(repository_module, "METRIC_BATCH_SIZE", 3)
    units = (Unit("summary", "summary", "ACTIVE"),) + tuple(
        Unit(
            "signal",
            f"s{i}",
            "COMPUTED",
            parent_unit_key="summary",
            metrics=(Metric("value", "number", Decimal(i)),),
        )
        for i in range(10)
    )
    with manager.get_session() as session:
        Repository.append_evaluation(original, session=session)
        run_id = Repository.append_run(
            _run(original.evaluation_id, units=units, status="ACTIVE"), session=session
        )
    ids = []
    after = 0
    while True:
        page = Repository.get_units_page(run_id, after_id=after, page_size=3)
        ids += [item["id"] for item in page["items"]]
        after = page["next_cursor"]
        if after is None:
            break
    assert len(set(ids)) == 11


def test_migration_offline_is_fixed_and_keeps_operational_population():
    module = import_module(
        "infrastructure.persistence.alembic.versions.20261012_01_immutable_pillar_evaluations"
    )
    buffer = StringIO()
    context = MigrationContext.configure(
        url="postgresql://", opts={"as_sql": True, "output_buffer": buffer}
    )
    with Operations.context(context):
        module.upgrade()
    sql = buffer.getvalue()
    assert "CREATE TABLE pillar_evaluations" in sql
    assert "NUMERIC(" not in next(
        ddl for ddl in module.DDL if "CREATE TABLE pillar_mining_metric_values" in ddl
    )
    assert "result_available_at" not in sql
    assert "DROP TABLE IF EXISTS pillar_mining_runs" in sql
    assert "DROP MATERIALIZED VIEW" not in sql
    assert "available_at" not in sql
    assert "recorded_at" not in sql
