from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from sqlalchemy import text

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import Event, PillarMiningObservation, Result
from infrastructure.persistence.repositories import pillar_mining_repository
from infrastructure.persistence.repositories.pillar_mining_repository import (
    PillarMiningObservationRepository,
)
from modules.pillars.mining.contracts import (
    PillarMiningObservation as MiningContract,
)


def _contract(**overrides) -> MiningContract:
    values = {
        "event_id": 1,
        "pillar_id": "pillar_2_side_market",
        "result_scope": "side_market",
        "module_id": "p2_raw_engine",
        "engine_version": "p2_raw_v1",
        "payload_schema_version": 1,
        "evaluation_minute": 5,
        "target_minute": 5,
        "observation_slot": "target:5",
        "sport": "Football",
        "competition_id": None,
        "market_type": "1X2",
        "status": "ACTIVE",
        "is_successful": True,
        "is_valid": None,
        "score_name": "SIDE_MARKET_EDGE",
        "score": Decimal("0.10"),
        "direction": "HOME",
        "strength": None,
        "metrics": {"SIDE_MARKET_EDGE": 0.10},
        "context": {"minutes_to_start": 5},
        "inputs": {"PIN_HOME": 2.0},
        "diagnostics": {"input_trace": {"PIN_HOME": {"quote_id": 1}}},
        "calculated_at": datetime(2026, 8, 22, 17, 55),
    }
    values.update(overrides)
    return MiningContract(**values)


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


def test_repository_upserts_by_slot_and_keeps_other_slots_and_versions(
    tmp_path,
    monkeypatch,
) -> None:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'mining.db'}")
    manager.create_tables()
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)

    with manager.get_session() as session:
        session.execute(text("PRAGMA foreign_keys = ON"))
        session.add(_event())

    original = _contract()
    PillarMiningObservationRepository.upsert(original)
    PillarMiningObservationRepository.upsert(
        replace(
            original,
            score=Decimal("0.25"),
            direction="AWAY",
            metrics={"SIDE_MARKET_EDGE": 0.25},
        )
    )
    PillarMiningObservationRepository.upsert(
        replace(
            original,
            target_minute=0,
            evaluation_minute=0,
            observation_slot="target:0",
        )
    )
    PillarMiningObservationRepository.upsert(
        replace(original, engine_version="p2_raw_v2")
    )

    with manager.get_session() as session:
        rows = (
            session.query(PillarMiningObservation)
            .order_by(PillarMiningObservation.id)
            .all()
        )
        assert len(rows) == 3
        updated = next(
            row
            for row in rows
            if row.observation_slot == "target:5"
            and row.engine_version == "p2_raw_v1"
        )
        assert updated.score == Decimal("0.250000000000")
        assert updated.direction == "AWAY"
        assert updated.metrics["SIDE_MARKET_EDGE"] == 0.25


def test_mining_observation_joins_results_and_cascades_with_event(
    tmp_path,
    monkeypatch,
) -> None:
    manager = DatabaseManager(f"sqlite:///{tmp_path / 'mining_join.db'}")
    manager.create_tables()
    monkeypatch.setattr(pillar_mining_repository, "db_manager", manager)

    with manager.get_session() as session:
        session.execute(text("PRAGMA foreign_keys = ON"))
        session.add(_event())
        session.add(Result(event_id=1, home_score=2, away_score=0, winner="1"))

    PillarMiningObservationRepository.upsert(_contract())

    with manager.get_session() as session:
        joined = (
            session.query(PillarMiningObservation.direction, Result.winner)
            .join(Result, Result.event_id == PillarMiningObservation.event_id)
            .one()
        )
        assert joined == ("HOME", "1")

        event = session.query(Event).filter(Event.id == 1).one()
        session.delete(event)

    with manager.get_session() as session:
        assert session.query(PillarMiningObservation).count() == 0
        assert session.query(Result).count() == 0
