"""SQL populations and frozen audit pages; historical populations stay in SQL."""

from __future__ import annotations
from contextlib import nullcontext
from datetime import datetime
from uuid import uuid4
from sqlalchemy import Numeric, bindparam, text
from infrastructure.persistence.models import P5MemorySample, P5MemorySampleMember
from infrastructure.persistence.types import UTCDateTime
from modules.pillars.evaluation_contracts import POLICY_VERSION
from modules.pillars.pillar_5.calculation_models import MemorySample


class Pillar5PriceMemoryRepository:
    def __init__(self, session_factory=None, *, session=None, capture=False):
        if capture and session is None:
            raise ValueError("audit capture requires the result transaction")
        self._session_factory = session_factory
        self._session = session
        self._capture = capture

    @staticmethod
    def _population(key, current_event_id, current_starts_at, population_filters):
        conditions = [
            "sport = :sport",
            "bookie_id = :bookie_id",
            "market_group = :market_group",
            "market_period = :market_period",
            "has_draw = :has_draw",
            "odds_home = :odds_home",
            "odds_away = :odds_away",
            "event_id != :current_event_id",
            "starts_at < :cutoff",
        ]
        params = {
            **key.to_dict(),
            "odds_home": key.odds_home,
            "odds_draw": key.odds_draw,
            "odds_away": key.odds_away,
            "current_event_id": current_event_id,
            "cutoff": current_starts_at,
        }
        conditions.append(
            "odds_draw = :odds_draw" if key.has_draw else "odds_draw IS NULL"
        )
        for name, value in population_filters.to_dict().items():
            if value is not None:
                conditions.append(f"{name} = :{name}")
                params[name] = value
        where = " AND ".join(conditions)
        sql = f"""WITH candidates AS (
            SELECT *, CASE
                WHEN UPPER(TRIM(winner_side)) IN ('1','HOME') THEN 'HOME'
                WHEN UPPER(TRIM(winner_side)) IN ('2','AWAY') THEN 'AWAY'
                WHEN :has_draw AND UPPER(TRIM(winner_side)) IN ('X','DRAW') THEN 'DRAW'
                ELSE NULL END AS outcome
            FROM mv_p5_price_memory WHERE {where}
        ), valid_rows AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY starts_at DESC, last_sync_at DESC) AS rn
            FROM candidates WHERE outcome IS NOT NULL AND home_score IS NOT NULL AND away_score IS NOT NULL
        ), eligible AS (SELECT * FROM valid_rows WHERE rn = 1) """
        return sql, params

    @staticmethod
    def _statement(sql):
        statement = text(sql)
        for name in ("odds_home", "odds_away", "odds_draw"):
            if ":" + name in sql:
                statement = statement.bindparams(bindparam(name, type_=Numeric(8, 3)))
        if ":cutoff" in sql:
            statement = statement.bindparams(bindparam("cutoff", type_=UTCDateTime()))
        return statement

    def summarize(
        self, *, key, current_event_id, current_starts_at, population_filters
    ):
        sql, params = self._population(
            key, current_event_id, current_starts_at, population_filters
        )
        scope = (
            nullcontext(self._session)
            if self._session is not None
            else self._session_factory()
        )
        with scope as session:
            if self._capture and session.get_bind().dialect.name == "sqlite":
                connection = session.connection()
                if not connection.connection.driver_connection.in_transaction:
                    connection.exec_driver_sql("BEGIN")
            # Per-profile savepoints isolate a failing lookup without discarding other profiles.
            with session.begin_nested():
                sample_id = str(uuid4()) if self._capture else None
                if sample_id:
                    header = P5MemorySample(
                        sample_id=sample_id,
                        cutoff=current_starts_at,
                        policy_version=POLICY_VERSION,
                        query={
                            "key": key.to_dict(),
                            "filters": population_filters.to_dict(),
                            "event_id": current_event_id,
                        },
                        sample_size=0,
                        wins_home=0,
                        wins_draw=0,
                        wins_away=0,
                    )
                    session.add(header)
                    session.flush()

                    # INSERT…SELECT bypasses ORM datetime bind processors. Keep
                    # SQLite's frozen timestamps in the same microsecond format
                    # used by UTCDateTime cursor binds; PostgreSQL has typed timestamps.
                    def timestamp_column(name):
                        if session.get_bind().dialect.name != "sqlite":
                            return name
                        return f"substr(replace(replace({name},'T',' '),'+00:00','') || CASE WHEN instr({name},'.')=0 THEN '.000000' ELSE '000000' END,1,26)"

                    starts_column = timestamp_column("starts_at")
                    sync_column = timestamp_column("last_sync_at")
                    session.execute(
                        self._statement(
                            sql
                            + """INSERT INTO p5_memory_sample_members
                        (sample_id,event_id,starts_at,competition_id,season_id,country,odds_home,odds_draw,odds_away,
                         home_score,away_score,winner_side,outcome,last_sync_at)
                        SELECT :sample_id,event_id,"""
                            + starts_column
                            + """,competition_id,season_id,country,odds_home,odds_draw,odds_away,
                               home_score,away_score,winner_side,outcome,"""
                            + sync_column
                            + " FROM eligible"
                        ),
                        {**params, "sample_id": sample_id},
                    )
                    counts_sql = "SELECT COUNT(*) AS n, SUM(CASE WHEN outcome='HOME' THEN 1 ELSE 0 END) AS home, SUM(CASE WHEN outcome='DRAW' THEN 1 ELSE 0 END) AS draw, SUM(CASE WHEN outcome='AWAY' THEN 1 ELSE 0 END) AS away FROM p5_memory_sample_members WHERE sample_id=:sample_id"
                    counts = (
                        session.execute(text(counts_sql), {"sample_id": sample_id})
                        .mappings()
                        .one()
                    )
                    (
                        header.sample_size,
                        header.wins_home,
                        header.wins_draw,
                        header.wins_away,
                    ) = (int(counts[k] or 0) for k in ("n", "home", "draw", "away"))
                else:
                    counts_sql = (
                        sql
                        + "SELECT COUNT(*) AS n, SUM(CASE WHEN outcome='HOME' THEN 1 ELSE 0 END) AS home, SUM(CASE WHEN outcome='DRAW' THEN 1 ELSE 0 END) AS draw, SUM(CASE WHEN outcome='AWAY' THEN 1 ELSE 0 END) AS away FROM eligible"
                    )
                    counts = (
                        session.execute(self._statement(counts_sql), params)
                        .mappings()
                        .one()
                    )
                excluded = (
                    session.execute(
                        self._statement(
                            sql + """SELECT
                    (SELECT COUNT(*) FROM candidates WHERE outcome IS NULL) AS incompatible_result,
                    (SELECT COUNT(*) FROM candidates WHERE outcome IS NOT NULL AND (home_score IS NULL OR away_score IS NULL)) AS missing_score,
                    (SELECT COUNT(*) FROM valid_rows WHERE rn > 1) AS duplicate_event"""
                        ),
                        params,
                    )
                    .mappings()
                    .one()
                )
                return MemorySample(
                    key=key,
                    sample_size=int(counts["n"]),
                    wins_home=int(counts["home"] or 0),
                    wins_draw=int(counts["draw"] or 0),
                    wins_away=int(counts["away"] or 0),
                    exclusions=dict(excluded),
                    sample_id=sample_id,
                )

    def get_sample_page(self, sample_id, cursor=None, page_size=100):
        if (
            isinstance(page_size, bool)
            or not isinstance(page_size, int)
            or not 1 <= page_size <= 1000
        ):
            raise ValueError("page_size must be between 1 and 1000")
        scope = (
            nullcontext(self._session)
            if self._session is not None
            else self._session_factory()
        )
        with scope as session:
            sample = session.get(P5MemorySample, sample_id)
            if sample is None or sample.run_id is None:
                raise LookupError("persisted sample not found")
            query = session.query(P5MemorySampleMember).filter_by(sample_id=sample_id)
            if cursor:
                date, event_id = cursor
                if isinstance(date, str):
                    date = datetime.fromisoformat(date)
                query = query.filter(
                    (P5MemorySampleMember.starts_at < date)
                    | (
                        (P5MemorySampleMember.starts_at == date)
                        & (P5MemorySampleMember.event_id < event_id)
                    )
                )
            rows = (
                query.order_by(
                    P5MemorySampleMember.starts_at.desc(),
                    P5MemorySampleMember.event_id.desc(),
                )
                .limit(page_size + 1)
                .all()
            )
            more = len(rows) > page_size
            members = rows[:page_size]

            def serialize(row):
                return {
                    column.name: (
                        value.isoformat()
                        if isinstance(value, datetime)
                        else (
                            str(value)
                            if column.name.startswith("odds_") and value is not None
                            else value
                        )
                    )
                    for column in P5MemorySampleMember.__table__.columns
                    if (value := getattr(row, column.name)) is not None
                }

            return {
                "sample_id": sample_id,
                "run_id": sample.run_id,
                "query": sample.query,
                "cutoff": sample.cutoff.isoformat(),
                "policy_version": sample.policy_version,
                "engine_version": sample.query["engine_version"],
                "payload_schema_version": sample.query["payload_schema_version"],
                "sample_size": sample.sample_size,
                "counts": {
                    "home": sample.wins_home,
                    "draw": sample.wins_draw,
                    "away": sample.wins_away,
                },
                "items": [serialize(row) for row in members],
                "next_cursor": (
                    (members[-1].starts_at.isoformat(), members[-1].event_id)
                    if more
                    else None
                ),
            }
