"""SQLAlchemy adapter for hierarchical pillar mining persistence."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from itertools import islice
from typing import Any

from sqlalchemy import JSON, delete, insert, select, text

from shared.temporal import utc_now

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    PillarMiningMetricValue as MiningMetricModel,
    PillarMiningRun as MiningRunModel,
    PillarMiningUnit as MiningUnitModel,
)
from modules.pillars.mining.contracts import (
    PillarMiningRun,
    iter_unit_layers,
)


def _supports_copy(session) -> bool:
    dialect = session.get_bind().dialect
    return dialect.name == 'postgresql' and dialect.driver == 'psycopg'


def _reserve_unit_ids(session, units) -> dict[str, int]:
    """Allocate IDs from the table's own sequence without predicting its values.

    Only IDs are retained, not additional ORM objects or serialized payloads.
    As with ordinary INSERT, sequence allocation is not undone by a rollback.
    """
    ids = session.scalars(text(
        "SELECT nextval(pg_get_serial_sequence('pillar_mining_units', 'id')) "
        "FROM generate_series(1, :count)"
    ), {'count': len(units)})
    return {unit.unit_key: unit_id for unit, unit_id in zip(units, ids, strict=True)}


def _copy_rows(session, table, rows) -> None:
    """Stream one bounded batch; COPY retains FK, unique and check constraints."""
    from psycopg import sql
    from psycopg.types.json import Jsonb

    columns = tuple(rows[0])
    json_columns = {name for name in columns if isinstance(table.c[name].type, JSON)}
    identity = sql.Identifier(table.schema, table.name) if table.schema else sql.Identifier(table.name)
    statement = sql.SQL('COPY {} ({}) FROM STDIN').format(
        identity, sql.SQL(', ').join(map(sql.Identifier, columns)),
    )
    # Reuse exactly the transaction owned by SQLAlchemy. Never commit or open
    # another connection here: P5 samples and the replaced graph stay atomic.
    connection = session.connection().connection.driver_connection
    with connection.cursor() as cursor:
        with cursor.copy(statement) as stream:
            for row in rows:
                stream.write_row(tuple(
                    Jsonb(row[name]) if name in json_columns else row[name]
                    for name in columns
                ))


# 1,000 units leave headroom below PostgreSQL's bound-parameter limit. Neither
# the ORM identity map nor a list of all scalar metrics grows with the run.
UNIT_BATCH_SIZE = 1000
METRIC_BATCH_SIZE = 1000


def _chunks(values, size):
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def _unit_values(run_id, unit, parent_id, now):
    raw_market_type = unit.dimensions.get('market_type_id') or unit.payload.get('market_type_id')
    try:
        market_type_id = int(raw_market_type) if raw_market_type is not None else None
    except (TypeError, ValueError):
        market_type_id = None
    try:
        line_value = Decimal(str(unit.line_value)) if unit.line_value not in (None, '') else None
    except (InvalidOperation, TypeError, ValueError):
        line_value = None
    return {
        'run_id': run_id, 'parent_unit_id': parent_id,
        'unit_type': unit.unit_type, 'unit_key': unit.unit_key,
        'ordinal': unit.ordinal, 'module_id': unit.module_id,
        'producer_status': unit.producer_status, 'canonical_status': unit.canonical_status,
        'signal_axis': unit.signal_axis, 'is_valid': unit.is_valid,
        'score_name': unit.score_name, 'score': unit.score,
        'direction': unit.direction, 'strength': unit.strength,
        'target_minute': unit.target_minute, 'market_type_id': market_type_id,
        'line_value': line_value, 'choice_name': unit.choice_name,
        'bookie_id': unit.bookie_id, 'quote_id': unit.quote_id,
        'source': unit.source, 'exchange_side': unit.exchange_side,
        'exchange_level': unit.exchange_level, 'dimensions': unit.dimensions,
        'payload': unit.payload, 'diagnostics': unit.diagnostics,
        'created_at': now, 'updated_at': now,
    }


def _metric_values(unit_id, metric, now):
    return {
        'unit_id': unit_id, 'metric_name': metric.name,
        'metric_group': metric.group, 'value_type': metric.value_type,
        'numeric_value': metric.value if metric.value_type == 'number' else None,
        'text_value': metric.value if metric.value_type == 'text' else None,
        'boolean_value': metric.value if metric.value_type == 'boolean' else None,
        'created_at': now, 'updated_at': now,
    }


def _insert_metrics(session, table, values, use_copy):
    if use_copy:
        _copy_rows(session, table, values)
    else:
        # Explicit multi-row SQL also batches drivers whose executemany sends
        # one non-returning INSERT per row.
        session.execute(insert(table).values(values))


def _replace_mining_graph(session, run_id, units) -> None:
    """Replace children without a per-row flush, SELECT, or ORM model allocation."""
    unit_table = MiningUnitModel.__table__
    metric_table = MiningMetricModel.__table__
    # Resolve old IDs in SQL, avoiding a large Python list and IN parameter list.
    owned_ids = select(unit_table.c.id).where(unit_table.c.run_id == run_id)
    session.execute(delete(metric_table).where(metric_table.c.unit_id.in_(owned_ids)))
    session.execute(delete(unit_table).where(unit_table.c.run_id == run_id))
    use_copy = _supports_copy(session)
    ids_by_key = _reserve_unit_ids(session, units) if use_copy and units else {}
    metric_batch = []
    now = utc_now()
    statement = insert(unit_table).returning(unit_table.c.id, unit_table.c.unit_key)
    for layer in iter_unit_layers(units):
        for chunk in _chunks(layer, UNIT_BATCH_SIZE):
            values = [
                _unit_values(run_id, unit, ids_by_key.get(unit.parent_unit_key), now)
                for unit in chunk
            ]
            # RETURNING order is not guaranteed. Associate IDs by the persisted
            # unique key, never by zipping result rows with input rows.
            if use_copy:
                for row in values:
                    row['id'] = ids_by_key[row['unit_key']]
                _copy_rows(session, unit_table, values)
            else:
                ids_by_key.update(
                    (key, unit_id) for unit_id, key in session.execute(statement, values)
                )
            metric_rows = (
                _metric_values(ids_by_key[unit.unit_key], metric, now)
                for unit in chunk for metric in unit.metrics
            )
            for row in metric_rows:
                metric_batch.append(row)
                if len(metric_batch) == METRIC_BATCH_SIZE:
                    _insert_metrics(session, metric_table, metric_batch, use_copy)
                    metric_batch.clear()
    # Share one bounded metric buffer across unit batches. Sparse/blocked series
    # must not cause a network round trip for each small group of scalar values.
    if metric_batch:
        _insert_metrics(session, metric_table, metric_batch, use_copy)


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
        now = utc_now()
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
    def _upsert_run(cls, session, run: PillarMiningRun) -> int:
        # ON CONFLICT DO UPDATE holds the identity row lock until transaction
        # completion. Returning only its ID avoids reloading large JSON payloads.
        statement = cls._build_atomic_upsert(
            cls._run_values(run), session.get_bind().dialect.name
        ).returning(MiningRunModel.id)
        return session.execute(statement).scalar_one()

    @classmethod
    def _replace_in_session(cls, session, run: PillarMiningRun) -> int:
        session.flush()
        run_id = cls._upsert_run(session, run)
        _replace_mining_graph(session, run_id, run.units)
        if run.pillar_id == "pillar_5" and run.payload_schema_version == 4:
            from infrastructure.persistence.models import P5MemorySample

            sample_ids = [
                profile["sample_id"]
                for profile in run.output_payload.get("analysis", {}).values()
                if profile.get("sample_id")
            ]
            from infrastructure.persistence.models import P5MemorySampleMember

            owned = session.query(P5MemorySample.sample_id).filter(
                P5MemorySample.run_id == run_id
            )
            session.query(P5MemorySampleMember).filter(
                P5MemorySampleMember.sample_id.in_(owned)
            ).delete(synchronize_session=False)
            session.query(P5MemorySample).filter(
                P5MemorySample.run_id == run_id
            ).delete(synchronize_session=False)
            for sample_id in sample_ids:
                sample = session.get(P5MemorySample, sample_id)
                if sample is None or sample.run_id is not None:
                    raise ValueError(
                        "sample must be captured in this result transaction"
                    )
                sample.run_id = run_id
                sample.query = {
                    **sample.query,
                    "engine_version": run.engine_version,
                    "payload_schema_version": run.payload_schema_version,
                }
        session.flush()
        return run_id

    @classmethod
    def replace_run(cls, run: PillarMiningRun, *, session=None) -> None:
        if session is not None:
            cls._replace_in_session(session, run)
        else:
            with db_manager.get_session() as owned_session:
                cls._replace_in_session(owned_session, run)

    @classmethod
    def get_result(cls, run_id: int, *, session=None) -> dict[str, Any]:
        if session is None:
            with db_manager.get_session() as owned_session:
                return cls.get_result(run_id, session=owned_session)
        from modules.pillars.evaluation_contracts import read_stored_result
        from modules.pillars.mining.serialization import to_json_value

        run = session.get(MiningRunModel, run_id)
        if run is None:
            raise LookupError("mining result not found")
        payload = read_stored_result(run.output_payload, run.payload_schema_version)
        if run.payload_schema_version == 4:
            payload = {**payload, "inputs": run.inputs, "signals": []}
            rows = (
                session.query(MiningUnitModel, MiningMetricModel)
                .outerjoin(
                    MiningMetricModel, MiningMetricModel.unit_id == MiningUnitModel.id
                )
                .filter(
                    MiningUnitModel.run_id == run_id,
                    MiningUnitModel.unit_type == "signal",
                )
                .order_by(MiningUnitModel.ordinal)
                .all()
            )
            for unit, metric in rows:
                value = (
                    None
                    if metric is None
                    else (
                        metric.numeric_value
                        if metric.value_type == "number"
                        else (
                            metric.text_value
                            if metric.value_type == "text"
                            else metric.boolean_value
                        )
                    )
                )
                payload["signals"].append(
                    {
                        "key": unit.payload["signal_key"],
                        "status": {
                            "ACTIVE": "COMPUTED",
                            "INSUFFICIENT_DATA": "BLOCKED",
                            "ERROR": "ERROR",
                        }[unit.producer_status],
                        "value": to_json_value(value),
                        "input_refs": unit.payload["input_refs"],
                        "contract_refs": unit.payload["contract_refs"],
                        "reason": unit.diagnostics["reason"],
                        "evidence": unit.diagnostics["evidence"],
                    }
                )
        return {
            "payload_schema_version": run.payload_schema_version,
            "engine_version": run.engine_version,
            "producer_status": run.producer_status,
            "canonical_status": run.canonical_status,
            "result": payload,
        }
