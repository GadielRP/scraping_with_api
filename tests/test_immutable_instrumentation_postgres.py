"""Real PostgreSQL acceptance, isolated in a disposable UUID-named schema."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from importlib import import_module
import os
from unittest.mock import patch
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import PillarEvaluation, PillarMiningMetricValue
from infrastructure.persistence.repositories import (
    pillar_mining_repository as repo_module,
)
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningRepository,
)
from infrastructure.persistence.repositories.pillar_5_price_memory_repository import (
    Pillar5PriceMemoryRepository,
)
from infrastructure.persistence.repositories.outcome_observation_repository import (
    append_outcome_observation,
)
from modules.jobs.pre_start_check_job import pillar_pipeline
from modules.pillars.pillar_5.ports import MemorySamplePersistenceError
from tests.pillars.test_market_evaluation import START
from tests.test_p5_audit_repository import pipeline_context
from tests.test_pillar_mining_repository import snapshot, _run


@pytest.fixture
def postgres_manager():
    url = os.getenv("MINING_TEST_DATABASE_URL")
    if not url:
        pytest.skip("MINING_TEST_DATABASE_URL is required for PostgreSQL acceptance")
    schema = "mining_acceptance_" + uuid4().hex
    admin = create_engine(url)
    with admin.begin() as connection:
        connection.execute(text(f"CREATE SCHEMA {schema}"))
    scoped_url = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"})
    manager = DatabaseManager(scoped_url.render_as_string(hide_password=False))
    try:
        with manager.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE market_choice_snapshots(snapshot_id INTEGER PRIMARY KEY)"
                )
            )
            connection.execute(
                text("CREATE TABLE pillar_mining_runs(id INTEGER PRIMARY KEY)")
            )
            connection.execute(text("INSERT INTO pillar_mining_runs VALUES (17)"))
            revision = import_module(
                "infrastructure.persistence.alembic.versions.20261012_01_immutable_pillar_evaluations"
            )
            with Operations.context(MigrationContext.configure(connection)):
                revision.upgrade()
            assert (
                connection.execute(
                    text("SELECT count(*) FROM pillar_mining_runs")
                ).scalar()
                == 0
            )
            connection.execute(text("""CREATE TABLE mv_p5_price_memory (
                event_id INTEGER, sport TEXT, competition_id INTEGER, season_id INTEGER, country TEXT,
                bookie_id INTEGER, market_group TEXT, market_period TEXT, has_draw BOOLEAN, starts_at TIMESTAMPTZ,
                odds_home NUMERIC, odds_draw NUMERIC, odds_away NUMERIC, home_score INTEGER, away_score INTEGER,
                winner_side TEXT, last_sync_at TIMESTAMPTZ)"""))
            for i, winner in enumerate(("HOME", "HOME", "DRAW", "AWAY"), 1):
                connection.execute(
                    text(
                        """INSERT INTO mv_p5_price_memory VALUES
                    (:id,'Football',11,2026,'Mexico',302,'1X2','Full Time',true,:date,2,2,2,1,0,:winner,:date)"""
                    ),
                    {"id": i, "date": START - timedelta(days=i), "winner": winner},
                )
        yield manager
    finally:
        manager.engine.dispose()
        with admin.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        admin.dispose()


def test_postgres_migration_copy_immutability_and_reconstruction(
    postgres_manager, monkeypatch
):
    manager = postgres_manager
    repository = PillarMiningRepository
    monkeypatch.setattr(pillar_pipeline, "db_manager", manager)
    monkeypatch.setattr(repo_module, "db_manager", manager)
    monkeypatch.setattr(
        pillar_pipeline, "_is_pillar_competition_in_scope", lambda _: True
    )
    processor = pillar_pipeline.EventPillarProcessor(
        enabled_pillars={"pillar_1": False}
    )

    with patch.object(repo_module, "_copy_rows", wraps=repo_module._copy_rows) as copy:
        output = processor.process_event(pipeline_context())
        assert copy.call_count > 0
    stored = repository.get_evaluation(output["evaluation_id"])
    assert len(stored["pillars"]) == 6
    assert output["pillar_5"]["status"] == "ACTIVE"
    p5 = next(run for run in stored["pillars"] if run["pillar_id"] == "pillar_5")
    profile_unit = next(
        unit for unit in p5["units"]["items"] if unit["unit_type"] == "signal"
    )
    assert profile_unit["dimensions"]["market_group"] == "1X2"
    assert profile_unit["dimensions"]["market_period"] == "Full Time"
    sample_id = next(p["sample_id"] for p in output["pillar_5"]["analysis"].values())
    reader = Pillar5PriceMemoryRepository(manager.SessionLocal)
    page = reader.get_sample_page(sample_id)
    assert page["counts"] == {"home": 2, "draw": 1, "away": 1}

    original = snapshot()
    run = _run(original.evaluation_id)
    with manager.get_session() as session:
        repository.append_evaluation(original, session=session)
        run_id = repository.append_run(run, session=session)
    with manager.get_session() as session:
        repository.append_evaluation(original, session=session)
        assert repository.append_run(run, session=session) == run_id
        assert session.scalar(
            select(PillarMiningMetricValue.numeric_value).where(
                PillarMiningMetricValue.metric_name == "composite"
            )
        ) == Decimal("-.12345678901234567890123456789")
    with pytest.raises(ValueError, match="conflicts"):
        with manager.get_session() as session:
            repository.append_run(
                replace(run, output_payload={"changed": True}), session=session
            )

    with manager.get_session() as session:
        append_outcome_observation(session, 900, {"home_score": 1, "away_score": 1})
        append_outcome_observation(session, 900, {"home_score": 2, "away_score": 1})
        session.execute(text("DELETE FROM mv_p5_price_memory"))
    case = repository.get_mining_case(output["evaluation_id"])
    assert case["evaluation"] == stored
    assert case["outcome"]["payload"]["home_score"] == 2
    assert reader.get_sample_page(sample_id) == page

    for table in (
        "pillar_evaluations",
        "pillar_mining_runs",
        "pillar_mining_units",
        "pillar_mining_metric_values",
        "event_outcome_observations",
        "p5_memory_samples",
        "p5_memory_sample_members",
    ):
        with pytest.raises(DBAPIError, match="immutable"):
            with manager.get_session() as session:
                session.execute(text(f"DELETE FROM {table}"))

    rollback_id = str(uuid4())
    with pytest.raises(RuntimeError, match="abort evaluation"):
        with manager.get_session() as session:
            repository.append_evaluation(snapshot(rollback_id), session=session)
            repository.append_run(_run(rollback_id), session=session)
            raise RuntimeError("abort evaluation")
    with manager.get_session() as session:
        assert session.get(PillarEvaluation, rollback_id) is None
        count_before = session.query(PillarEvaluation).count()
        session.execute(text("DROP TABLE p5_memory_sample_members"))
    with pytest.raises(MemorySamplePersistenceError):
        processor.process_event(pipeline_context())
    with manager.get_session() as session:
        assert session.query(PillarEvaluation).count() == count_before
