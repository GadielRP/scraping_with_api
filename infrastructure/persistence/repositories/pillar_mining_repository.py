"""SQLAlchemy adapter for hierarchical pillar mining persistence."""

from __future__ import annotations

from typing import Any

from shared.timezone_utils import get_local_now

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    PillarMiningMetricValue as MiningMetricModel,
    PillarMiningRun as MiningRunModel,
    PillarMiningUnit as MiningUnitModel,
)
from modules.pillars.mining.contracts import (
    PillarMiningMetric,
    PillarMiningRun,
    PillarMiningUnit,
)


class PillarMiningRepository:
    """Atomically replace the complete child graph of a canonical run."""

    _IDENTITY_COLUMNS = (
        "event_id",
        "pillar_id",
        "result_scope",
        "execution_slot",
        "engine_version",
    )

    @staticmethod
    def _run_values(run: PillarMiningRun) -> dict[str, Any]:
        now = get_local_now()
        return {
            "event_id": run.event_id,
            "pillar_id": run.pillar_id,
            "result_scope": run.result_scope,
            "execution_slot": run.execution_slot,
            "engine_version": run.engine_version,
            "payload_schema_version": run.payload_schema_version,
            "producer_status": run.producer_status,
            "canonical_status": run.canonical_status,
            "evaluation_minute": run.evaluation_minute,
            "target_minute": run.target_minute,
            "calculated_at": run.calculated_at,
            "sport": run.sport,
            "competition_id": run.competition_id,
            "context": run.context,
            "inputs": run.inputs,
            "diagnostics": run.diagnostics,
            "output_payload": run.output_payload,
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def _unit_values(
        run_id: int,
        unit: PillarMiningUnit,
        parent_unit_id: int | None,
    ) -> dict[str, Any]:
        now = get_local_now()
        return {
            "run_id": run_id,
            "parent_unit_id": parent_unit_id,
            "unit_type": unit.unit_type,
            "unit_key": unit.unit_key,
            "ordinal": unit.ordinal,
            "module_id": unit.module_id,
            "producer_status": unit.producer_status,
            "canonical_status": unit.canonical_status,
            "signal_axis": unit.signal_axis,
            "is_valid": unit.is_valid,
            "score_name": unit.score_name,
            "score": unit.score,
            "direction": unit.direction,
            "strength": unit.strength,
            "target_minute": unit.target_minute,
            "market_group": unit.market_group,
            "market_period": unit.market_period,
            "market_name": unit.market_name,
            "choice_group": unit.choice_group,
            "choice_name": unit.choice_name,
            "bookie_id": unit.bookie_id,
            "quote_id": unit.quote_id,
            "source": unit.source,
            "exchange_side": unit.exchange_side,
            "exchange_level": unit.exchange_level,
            "dimensions": unit.dimensions,
            "payload": unit.payload,
            "diagnostics": unit.diagnostics,
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def _metric_model(unit_id: int, metric: PillarMiningMetric) -> MiningMetricModel:
        values: dict[str, Any] = {
            "unit_id": unit_id,
            "metric_name": metric.name,
            "metric_group": metric.group,
            "value_type": metric.value_type,
        }
        if metric.value_type == "number":
            values["numeric_value"] = metric.value
        elif metric.value_type == "text":
            values["text_value"] = metric.value
        else:
            values["boolean_value"] = metric.value
        return MiningMetricModel(**values)

    @classmethod
    def _build_atomic_upsert(cls, values: dict[str, Any], dialect_name: str):
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif dialect_name == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise ValueError(f"atomic mining upsert is unsupported for {dialect_name!r}")

        statement = insert(MiningRunModel).values(values)
        update_values = {
            key: getattr(statement.excluded, key)
            for key in values
            if key not in {*cls._IDENTITY_COLUMNS, "created_at"}
        }
        return statement.on_conflict_do_update(
            index_elements=list(cls._IDENTITY_COLUMNS),
            set_=update_values,
        )

    @classmethod
    def _upsert_run(cls, session, run: PillarMiningRun) -> MiningRunModel:
        values = cls._run_values(run)
        identity = {key: values[key] for key in cls._IDENTITY_COLUMNS}
        dialect_name = session.get_bind().dialect.name

        if dialect_name in {"postgresql", "sqlite"}:
            session.execute(cls._build_atomic_upsert(values, dialect_name))
        else:
            existing = session.query(MiningRunModel).filter_by(**identity).one_or_none()
            if existing is None:
                session.add(MiningRunModel(**values))
            else:
                for key, value in values.items():
                    if key not in {*cls._IDENTITY_COLUMNS, "created_at"}:
                        setattr(existing, key, value)

        session.flush()
        query = session.query(MiningRunModel).filter_by(**identity)
        if dialect_name == "postgresql":
            query = query.with_for_update()
        return query.one()

    @staticmethod
    def _ordered_units(units: tuple[PillarMiningUnit, ...]) -> list[PillarMiningUnit]:
        """Topologically order units while preserving sibling declaration order."""

        pending = list(units)
        ordered: list[PillarMiningUnit] = []
        inserted_keys: set[str] = set()
        while pending:
            ready = [
                unit
                for unit in pending
                if unit.parent_unit_key is None
                or unit.parent_unit_key in inserted_keys
            ]
            if not ready:
                raise ValueError("mining unit graph contains an unresolved parent or cycle")
            for unit in ready:
                pending.remove(unit)
                ordered.append(unit)
                inserted_keys.add(unit.unit_key)
        return ordered

    @classmethod
    def replace_run(cls, run: PillarMiningRun) -> None:
        with db_manager.get_session() as session:
            run_model = cls._upsert_run(session, run)

            existing_unit_ids = [
                unit_id
                for (unit_id,) in session.query(MiningUnitModel.id)
                .filter(MiningUnitModel.run_id == run_model.id)
                .all()
            ]
            if existing_unit_ids:
                session.query(MiningMetricModel).filter(
                    MiningMetricModel.unit_id.in_(existing_unit_ids)
                ).delete(synchronize_session=False)
                session.query(MiningUnitModel).filter(
                    MiningUnitModel.run_id == run_model.id
                ).delete(synchronize_session=False)
                session.flush()

            ids_by_key: dict[str, int] = {}
            for unit in cls._ordered_units(run.units):
                parent_id = (
                    ids_by_key[unit.parent_unit_key]
                    if unit.parent_unit_key is not None
                    else None
                )
                unit_model = MiningUnitModel(
                    **cls._unit_values(run_model.id, unit, parent_id)
                )
                session.add(unit_model)
                session.flush()
                ids_by_key[unit.unit_key] = unit_model.id
                session.add_all(
                    cls._metric_model(unit_model.id, metric)
                    for metric in unit.metrics
                )
