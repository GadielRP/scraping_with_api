"""SQLAlchemy adapter for hierarchical pillar mining persistence."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from itertools import islice

from sqlalchemy import JSON, insert, select, text

from shared.temporal import utc_now
from modules.pillars.mining.serialization import to_json_value

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    PillarEvaluation,
    EventOutcomeObservation,
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
    return dialect.name == "postgresql" and dialect.driver == "psycopg"


def _reserve_unit_ids(session, units) -> dict[str, int]:
    """Allocate IDs from the table's own sequence without predicting its values.

    Only IDs are retained, not additional ORM objects or serialized payloads.
    As with ordinary INSERT, sequence allocation is not undone by a rollback.
    """
    ids = session.scalars(
        text(
            "SELECT nextval(pg_get_serial_sequence('pillar_mining_units', 'id')) "
            "FROM generate_series(1, :count)"
        ),
        {"count": len(units)},
    )
    return {unit.unit_key: unit_id for unit, unit_id in zip(units, ids, strict=True)}


def _copy_rows(session, table, rows) -> None:
    """Stream one bounded batch; COPY retains FK, unique and check constraints."""
    from psycopg import sql
    from psycopg.types.json import Jsonb

    columns = tuple(rows[0])
    json_columns = {name for name in columns if isinstance(table.c[name].type, JSON)}
    identity = (
        sql.Identifier(table.schema, table.name)
        if table.schema
        else sql.Identifier(table.name)
    )
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        identity,
        sql.SQL(", ").join(map(sql.Identifier, columns)),
    )
    # Reuse exactly the transaction owned by SQLAlchemy. Never commit or open
    # another connection here: P5 samples and the immutable graph stay atomic.
    connection = session.connection().connection.driver_connection
    with connection.cursor() as cursor:
        with cursor.copy(statement) as stream:
            for row in rows:
                stream.write_row(
                    tuple(
                        Jsonb(row[name]) if name in json_columns else row[name]
                        for name in columns
                    )
                )


# 1,000 units leave headroom below PostgreSQL's bound-parameter limit. Neither
# the ORM identity map nor a list of all scalar metrics grows with the run.
UNIT_BATCH_SIZE = 1000
METRIC_BATCH_SIZE = 1000


def _chunks(values, size):
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def _unit_values(run_id, unit, parent_id, now):
    raw_market_type = unit.dimensions.get("market_type_id") or unit.payload.get(
        "market_type_id"
    )
    try:
        market_type_id = int(raw_market_type) if raw_market_type is not None else None
    except (TypeError, ValueError):
        market_type_id = None
    try:
        line_value = (
            Decimal(str(unit.line_value)) if unit.line_value not in (None, "") else None
        )
    except (InvalidOperation, TypeError, ValueError):
        line_value = None
    if line_value is not None and not line_value.is_finite():
        line_value = None
    return {
        "run_id": run_id,
        "parent_unit_id": parent_id,
        "unit_type": unit.unit_type,
        "unit_key": unit.unit_key,
        "ordinal": unit.ordinal,
        "module_id": unit.module_id,
        "status": unit.status,
        "reason": unit.diagnostics.get("reason"),
        "signal_axis": unit.signal_axis,
        "is_valid": unit.is_valid,
        "score_name": unit.score_name,
        "score": unit.score,
        "direction": unit.direction,
        "strength": unit.strength,
        "target_minute": unit.target_minute,
        "market_type_id": market_type_id,
        "line_value": line_value,
        "choice_name": unit.choice_name,
        "bookie_id": unit.bookie_id,
        "quote_id": unit.quote_id,
        "source": unit.source,
        "exchange_side": unit.exchange_side,
        "exchange_level": unit.exchange_level,
        "dimensions": to_json_value(
            {
                **unit.dimensions,
                **{
                    key: value
                    for key, value in (
                        ("market_group", unit.market_group),
                        ("market_period", unit.market_period),
                        ("market_name", unit.market_name),
                    )
                    if value is not None
                },
            }
        ),
        "payload": to_json_value(unit.payload),
        "diagnostics": to_json_value(unit.diagnostics),
        "created_at": now,
    }


def _metric_values(unit_id, metric, now):
    return {
        "unit_id": unit_id,
        "metric_name": metric.name,
        "metric_group": metric.group,
        "value_type": metric.value_type,
        "numeric_value": metric.value if metric.value_type == "number" else None,
        "text_value": metric.value if metric.value_type == "text" else None,
        "boolean_value": metric.value if metric.value_type == "boolean" else None,
        "created_at": now,
    }


def _insert_metrics(session, table, values, use_copy):
    if use_copy:
        _copy_rows(session, table, values)
    else:
        # Explicit multi-row SQL also batches drivers whose executemany sends
        # one non-returning INSERT per row.
        session.execute(insert(table).values(values))


def _insert_mining_graph(session, run_id, units) -> None:
    """Append children without per-row flushes or ORM model allocation."""
    unit_table = MiningUnitModel.__table__
    metric_table = MiningMetricModel.__table__
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
                    row["id"] = ids_by_key[row["unit_key"]]
                _copy_rows(session, unit_table, values)
            else:
                ids_by_key.update(
                    (key, unit_id)
                    for unit_id, key in session.execute(statement, values)
                )
            metric_rows = (
                _metric_values(ids_by_key[unit.unit_key], metric, now)
                for unit in chunk
                for metric in unit.metrics
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
    """Append-only evaluations; no operational foreign key can rewrite history."""

    @staticmethod
    def _insert_once(session, model, values, identity):
        if session.get_bind().dialect.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        else:
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        primary = list(model.__table__.primary_key.columns)[0]
        statement = (
            dialect_insert(model)
            .values(**values)
            .on_conflict_do_nothing(index_elements=list(identity))
            .returning(primary)
        )
        inserted = session.execute(statement).scalar_one_or_none()
        if inserted is not None:
            return inserted, True
        stored = session.execute(
            select(primary, model.content_hash).where(
                *(getattr(model, key) == values[key] for key in identity)
            )
        ).one()
        if stored.content_hash != values["content_hash"]:
            raise ValueError(
                "immutable evaluation content conflicts with an existing identity"
            )
        return stored[0], False

    @classmethod
    def append_evaluation(cls, snapshot, *, session):
        from modules.pillars.mining.fingerprint import content_hash
        from modules.pillars.mining.serialization import to_json_value

        values = to_json_value(snapshot, copy_containers=False)
        digest = content_hash(values)
        values["evaluation_as_of"] = snapshot.evaluation_as_of
        values["content_hash"] = digest
        return cls._insert_once(session, PillarEvaluation, values, ("evaluation_id",))[
            0
        ]

    @classmethod
    def append_run(cls, run: PillarMiningRun, *, session):
        from modules.pillars.mining.contracts import validate_mining_run
        from modules.pillars.mining.fingerprint import content_hash

        validate_mining_run(run)
        parent = session.execute(
            select(PillarEvaluation.event_id).where(
                PillarEvaluation.evaluation_id == run.evaluation_id
            )
        ).scalar_one()
        if parent != run.event_id:
            raise ValueError("run event does not match its evaluation")
        values = {
            key: getattr(run, key)
            for key in (
                "evaluation_id",
                "event_id",
                "pillar_id",
                "result_scope",
                "engine_version",
                "payload_schema_version",
                "status",
                "sport",
                "evaluation_minute",
                "target_minute",
                "competition_id",
                "context",
                "inputs",
                "diagnostics",
                "output_payload",
                "calculated_at",
            )
        }
        values["content_hash"] = content_hash(
            {key: value for key, value in vars(run).items() if key != "calculated_at"}
        )
        run_id, inserted = cls._insert_once(
            session,
            MiningRunModel,
            values,
            ("evaluation_id", "pillar_id", "result_scope"),
        )
        if not inserted:
            return run_id
        _insert_mining_graph(session, run_id, run.units)
        if run.pillar_id == "pillar_5":
            from infrastructure.persistence.models import P5MemorySample

            for profile in run.output_payload.get("analysis", {}).values():
                sample_id = profile.get("sample_id")
                if sample_id is None:
                    continue
                sample = session.get(P5MemorySample, sample_id)
                if sample is None or sample.run_id is not None:
                    raise ValueError(
                        "sample must be captured in this evaluation transaction"
                    )
                sample.run_id = run_id
                sample.query = {
                    **sample.query,
                    "engine_version": run.engine_version,
                    "payload_schema_version": run.payload_schema_version,
                }
        session.flush()
        return run_id

    @staticmethod
    def require_complete_evaluation(evaluation_id, *, session):
        scopes = set(
            session.execute(
                select(MiningRunModel.pillar_id, MiningRunModel.result_scope).where(
                    MiningRunModel.evaluation_id == evaluation_id
                )
            ).all()
        )
        expected = {
            ("pillar_1_team_structure", "side"),
            ("pillar_1_team_structure", "totals"),
            ("pillar_2_side_market", "side_market"),
            ("pillar_3_totals_market_context", "totals_market_context"),
            ("pillar_4_temporal_market_drift", "temporal_market_drift"),
            ("pillar_5", "price_memory"),
        }
        if scopes != expected:
            raise ValueError(
                f"evaluation must contain six independent scopes: {expected - scopes}"
            )

    @classmethod
    def get_units_page(cls, run_id, *, after_id=0, page_size=100, session=None):
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        if session is None:
            with db_manager.get_session() as owned:
                return cls.get_units_page(
                    run_id, after_id=after_id, page_size=page_size, session=owned
                )
        units = session.scalars(
            select(MiningUnitModel)
            .where(MiningUnitModel.run_id == run_id, MiningUnitModel.id > after_id)
            .order_by(MiningUnitModel.id)
            .limit(page_size + 1)
        ).all()
        more, units = len(units) > page_size, units[:page_size]
        metric_rows = session.scalars(
            select(MiningMetricModel)
            .where(MiningMetricModel.unit_id.in_([u.id for u in units]))
            .order_by(MiningMetricModel.id)
        )
        metrics = {}
        for metric in metric_rows:
            metrics.setdefault(metric.unit_id, []).append(
                {
                    "name": metric.metric_name,
                    "group": metric.metric_group,
                    "value_type": metric.value_type,
                    "value": to_json_value(
                        getattr(
                            metric,
                            {
                                "number": "numeric_value",
                                "text": "text_value",
                                "boolean": "boolean_value",
                            }[metric.value_type],
                        )
                    ),
                }
            )
        return {
            "items": [
                {
                    **to_json_value(
                        {
                            col.name: getattr(unit, col.name)
                            for col in MiningUnitModel.__table__.columns
                        }
                    ),
                    "metrics": metrics.get(unit.id, []),
                }
                for unit in units
            ],
            "next_cursor": units[-1].id if more else None,
        }

    @classmethod
    def get_result(cls, run_id, *, session=None):
        if session is None:
            with db_manager.get_session() as owned:
                return cls.get_result(run_id, session=owned)
        from modules.pillars.evaluation_contracts import PAYLOAD_SCHEMA_VERSION
        from modules.pillars.mining.serialization import to_json_value

        run = session.get(MiningRunModel, run_id)
        if run is None:
            raise LookupError("mining result not found")
        if run.payload_schema_version != PAYLOAD_SCHEMA_VERSION:
            raise ValueError("unsupported mining schema")
        payload = {**run.output_payload, "diagnostics": run.diagnostics}
        if "signal_unit_refs" in payload:
            shared = None
            payload["inputs"] = {}
            for name, point in run.inputs.items():
                ref = point.get("evaluation_input_ref")
                if ref is not None:
                    if shared is None:
                        shared = session.get(
                            PillarEvaluation, run.evaluation_id
                        ).inputs["selected_inputs"]
                    payload["inputs"][name] = shared[ref]
                else:
                    payload["inputs"][name] = point
            payload["signals"] = []
            rows = session.execute(
                select(MiningUnitModel, MiningMetricModel)
                .outerjoin(
                    MiningMetricModel, MiningMetricModel.unit_id == MiningUnitModel.id
                )
                .where(
                    MiningUnitModel.run_id == run_id,
                    MiningUnitModel.unit_type == "signal",
                )
                .order_by(MiningUnitModel.ordinal)
                .execution_options(yield_per=1000)
            )
            for unit, metric in rows:
                value = (
                    None
                    if metric is None
                    else getattr(
                        metric,
                        {
                            "number": "numeric_value",
                            "text": "text_value",
                            "boolean": "boolean_value",
                        }[metric.value_type],
                    )
                )
                payload["signals"].append(
                    {
                        "key": unit.payload["signal_key"],
                        "status": unit.status,
                        "value": to_json_value(value),
                        "input_refs": unit.payload.get("input_refs", []),
                        "contract_refs": unit.payload.get("contract_refs", []),
                        "reason": unit.diagnostics.get("reason"),
                        "evidence": unit.diagnostics.get("evidence", {}),
                    }
                )
        return {
            "run_id": run.id,
            "evaluation_id": run.evaluation_id,
            "pillar_id": run.pillar_id,
            "result_scope": run.result_scope,
            "payload_schema_version": run.payload_schema_version,
            "engine_version": run.engine_version,
            "status": run.status,
            "result": payload,
            "units": cls.get_units_page(run_id, session=session),
        }

    @classmethod
    def get_evaluation(cls, evaluation_id, *, session=None):
        if session is None:
            with db_manager.get_session() as owned:
                return cls.get_evaluation(evaluation_id, session=owned)
        snapshot = session.get(PillarEvaluation, evaluation_id)
        if snapshot is None:
            raise LookupError("evaluation not found")
        ids = session.scalars(
            select(MiningRunModel.id)
            .where(MiningRunModel.evaluation_id == evaluation_id)
            .order_by(MiningRunModel.id)
        )
        return {
            "evaluation_id": snapshot.evaluation_id,
            "event_id": snapshot.event_id,
            "evaluation_as_of": snapshot.evaluation_as_of.isoformat(),
            "schema_version": snapshot.schema_version,
            "context": snapshot.context,
            "inputs": snapshot.inputs,
            "configuration": snapshot.configuration,
            "versions": snapshot.versions,
            "pillars": [cls.get_result(run_id, session=session) for run_id in ids],
        }

    @classmethod
    def get_outcomes_page(cls, event_id, *, cursor=None, page_size=100, session=None):
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        if session is None:
            with db_manager.get_session() as owned:
                return cls.get_outcomes_page(
                    event_id, cursor=cursor, page_size=page_size, session=owned
                )
        query = select(EventOutcomeObservation).where(
            EventOutcomeObservation.event_id == event_id
        )
        if cursor:
            from datetime import datetime
            from sqlalchemy import tuple_

            received_at, observation_id = cursor
            query = query.where(
                tuple_(
                    EventOutcomeObservation.received_at,
                    EventOutcomeObservation.observation_id,
                )
                < tuple_(datetime.fromisoformat(received_at), observation_id)
            )
        rows = session.scalars(
            query.order_by(
                EventOutcomeObservation.received_at.desc(),
                EventOutcomeObservation.observation_id.desc(),
            ).limit(page_size + 1)
        ).all()
        more, rows = len(rows) > page_size, rows[:page_size]
        return {
            "items": [
                to_json_value(
                    {
                        col.name: getattr(row, col.name)
                        for col in EventOutcomeObservation.__table__.columns
                    }
                )
                for row in rows
            ],
            "next_cursor": (
                (rows[-1].received_at.isoformat(), rows[-1].observation_id)
                if more
                else None
            ),
        }

    @classmethod
    def get_mining_case(cls, evaluation_id):
        with db_manager.get_session() as session:
            evaluation = cls.get_evaluation(evaluation_id, session=session)
            outcome = session.scalars(
                select(EventOutcomeObservation)
                .where(EventOutcomeObservation.event_id == evaluation["event_id"])
                .order_by(
                    EventOutcomeObservation.received_at.desc(),
                    EventOutcomeObservation.observation_id.desc(),
                )
                .limit(1)
            ).first()
            return {
                "evaluation": evaluation,
                "outcome": (
                    None
                    if outcome is None
                    else {
                        "observation_id": outcome.observation_id,
                        "source": outcome.source,
                        "source_event_id": outcome.source_event_id,
                        "created_at": outcome.created_at.isoformat(),
                        "received_at": outcome.received_at.isoformat(),
                        "source_observed_at": (
                            outcome.source_observed_at.isoformat()
                            if outcome.source_observed_at
                            else None
                        ),
                        "payload": outcome.payload,
                    }
                ),
            }
