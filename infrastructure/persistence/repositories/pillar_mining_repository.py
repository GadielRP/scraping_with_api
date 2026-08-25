"""SQLAlchemy adapter for canonical pillar mining observations."""

from __future__ import annotations

from shared.timezone_utils import get_local_now

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import PillarMiningObservation as MiningModel
from modules.pillars.mining.contracts import PillarMiningObservation


class PillarMiningObservationRepository:
    _IDENTITY_COLUMNS = (
        "event_id",
        "pillar_id",
        "result_scope",
        "observation_slot",
        "engine_version",
    )

    @staticmethod
    def _values(observation: PillarMiningObservation) -> dict:
        now = get_local_now()
        return {
            "event_id": observation.event_id,
            "pillar_id": observation.pillar_id,
            "result_scope": observation.result_scope,
            "module_id": observation.module_id,
            "engine_version": observation.engine_version,
            "payload_schema_version": observation.payload_schema_version,
            "evaluation_minute": observation.evaluation_minute,
            "target_minute": observation.target_minute,
            "observation_slot": observation.observation_slot,
            "calculated_at": observation.calculated_at,
            "sport": observation.sport,
            "competition_id": observation.competition_id,
            "market_type": observation.market_type,
            "status": observation.status,
            "is_successful": observation.is_successful,
            "is_valid": observation.is_valid,
            "score_name": observation.score_name,
            "score": observation.score,
            "direction": observation.direction,
            "strength": observation.strength,
            "metrics": observation.metrics,
            "context": observation.context,
            "inputs": observation.inputs,
            "diagnostics": observation.diagnostics,
            "created_at": now,
            "updated_at": now,
        }

    @classmethod
    def upsert(cls, observation: PillarMiningObservation) -> None:
        values = cls._values(observation)
        with db_manager.get_session() as session:
            dialect_name = session.get_bind().dialect.name
            if dialect_name in {"postgresql", "sqlite"}:
                if dialect_name == "postgresql":
                    from sqlalchemy.dialects.postgresql import insert
                else:
                    from sqlalchemy.dialects.sqlite import insert

                statement = insert(MiningModel).values(values)
                update_values = {
                    key: getattr(statement.excluded, key)
                    for key in values
                    if key not in {*cls._IDENTITY_COLUMNS, "created_at"}
                }
                statement = statement.on_conflict_do_update(
                    index_elements=list(cls._IDENTITY_COLUMNS),
                    set_=update_values,
                )
                session.execute(statement)
                return

            identity = {
                key: values[key]
                for key in cls._IDENTITY_COLUMNS
            }
            existing = session.query(MiningModel).filter_by(**identity).one_or_none()
            if existing is None:
                session.add(MiningModel(**values))
                return

            for key, value in values.items():
                if key not in {*cls._IDENTITY_COLUMNS, "created_at"}:
                    setattr(existing, key, value)
