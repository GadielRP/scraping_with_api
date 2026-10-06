"""Isolated PostgreSQL load probe; never run against an application database.

Require an empty database whose name starts execution_load_. Providers are fixtures;
SQL writers and the two materialized-view definitions are the application code.
"""

import argparse
from datetime import date, datetime, timezone
from importlib import import_module
import json
import os
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10000)
    parser.add_argument("--history", type=int, default=20000)
    parser.add_argument("--output", default="scratch/execution-load.json")
    args = parser.parse_args()
    from sqlalchemy.engine import make_url

    url = os.environ["EXECUTION_LOAD_DATABASE_URL"]
    if not (make_url(url).database or "").startswith("execution_load_"):
        raise ValueError("Use an empty execution_load_* disposable database")
    os.environ["DATABASE_URL"] = url
    from sqlalchemy import inspect, text
    from infrastructure.persistence.database import db_manager as database
    from infrastructure.persistence.orm_base import Base
    from infrastructure.persistence.models import Participant, Competition, Bookie
    from infrastructure.persistence.odds_models import CanonicalMarketType
    from infrastructure.persistence.repositories.daily_discovery_repository import (
        DailyDiscoveryRepository,
    )
    from infrastructure.persistence.views.dual_process_views import (
        build_dual_process_event_odds_view_sql,
        MV_ALERT_EVENTS_SQL,
        MV_ALERT_EVENTS_INDEXES_SQL,
        DUAL_PROCESS_MARKET_INDEXES_SQL,
    )
    from infrastructure.persistence.views.p5_price_memory_view import (
        build_p5_price_memory_view_sql,
        MV_P5_PRICE_MEMORY_INDEXES_SQL,
    )
    from infrastructure.settings import Config
    from modules.jobs.daily_discovery import pipeline
    from modules.jobs.results_collection_job import batch_processor
    from modules.jobs.results_collection_job.run_results_collection_job import (
        run_results_collection,
    )
    from modules.jobs.view_refresh.run_view_refresh import run_view_refresh
    from infrastructure.scheduler.serial_executor import SerialExecutor
    from infrastructure.scheduler.contracts import JobRequest
    from shared.execution_context import Priority
    from shared.runtime_observability import get_rss_mb, get_cgroup_memory_snapshot
    from shared.temporal import utc_now

    if inspect(database.engine).get_table_names():
        raise ValueError("Benchmark database must be empty")
    Base.metadata.create_all(database.engine)
    with database.get_session() as session:
        session.add_all(
            [
                Participant(
                    participant_id=index, source="sofascore", source_participant_id=index, name=name
                )
                for index, name in ((1, "Home"), (2, "Away"))
            ]
        )
        session.add(
            Competition(
                competition_id=1,
                source="sofascore",
                source_tournament_id=50,
                source_unique_tournament_id=5,
                canonical_name="League",
                display_name="League",
            )
        )
        session.add(Bookie(bookie_id=1, name="SofaScore", slug="sofascore"))
        session.add(
            CanonicalMarketType(
                canonical_market_key="benchmark",
                market_type_id=1,
                canonical_market_name="1X2",
                canonical_market_group="1X2",
                canonical_market_period="Full Time",
                market_family="moneyline",
            )
        )
    with database.engine.begin() as connection:
        connection.execute(
            text(
                """INSERT INTO events(id,slug,starts_at,sport,competition,home_team,away_team,gender,
            discovery_source,alert_sent,home_participant_id,away_participant_id,competition_id)
            SELECT id,'history-'||id,'2026-10-01 12:00+00'::timestamptz,'Football','League','Home','Away','Men',
            'benchmark',false,1,2,1 FROM generate_series(1,:count) id"""
            ),
            {"count": args.history},
        )
        connection.execute(text("""INSERT INTO results(event_id,home_score,away_score,winner)
            SELECT id,2,0,'1' FROM events"""))
        connection.execute(
            text(
                """INSERT INTO markets(market_id,event_id,bookie_id,market_type_id,is_live,collected_at)
            SELECT id,id,1,1,false,'2026-09-30 12:00+00' FROM events"""
            )
        )
        connection.execute(text("""INSERT INTO market_choices(choice_id,market_id,choice_name)
            SELECT (e.id-1)*3+slot,e.id,CASE slot WHEN 1 THEN '1' WHEN 2 THEN 'X' ELSE '2' END
            FROM events e CROSS JOIN generate_series(1,3) slot"""))
        connection.execute(
            text(
                """INSERT INTO market_choice_quotes(quote_id,choice_id,source,exchange_level,initial_odds,
            current_odds,initial_captured_at,current_updated_at)
            SELECT choice_id,choice_id,'sofascore',0,2.0,2.1,'2026-09-30 12:00+00','2026-09-30 13:00+00'
            FROM market_choices"""
            )
        )
        connection.execute(
            text("""INSERT INTO market_choice_snapshots(quote_id,odds_value,collected_at)
            SELECT quote_id,2.1,'2026-09-30 13:00+00' FROM market_choice_quotes""")
        )
        for table, column in (
            ("events", "id"),
            ("participants", "participant_id"),
            ("competitions", "competition_id"),
            ("bookies", "bookie_id"),
            ("markets", "market_id"),
            ("market_choices", "choice_id"),
            ("market_choice_quotes", "quote_id"),
        ):
            connection.execute(
                text(
                    f"SELECT setval(pg_get_serial_sequence('{table}','{column}'),(SELECT max({column}) FROM {table}))"
                )
            )
        for statement in DUAL_PROCESS_MARKET_INDEXES_SQL:
            connection.execute(text(statement))
        connection.execute(text(build_dual_process_event_odds_view_sql(["1X2"], ["Full Time"])))
        connection.execute(text(MV_ALERT_EVENTS_SQL))
        connection.execute(text(build_p5_price_memory_view_sql()))
        for statement in MV_ALERT_EVENTS_INDEXES_SQL + MV_P5_PRICE_MEMORY_INDEXES_SQL:
            connection.execute(text(statement))
        # Exercise the actual migration's grants/function against these real views.
        from alembic.migration import MigrationContext
        from alembic.operations import Operations
        from infrastructure.persistence.reporting_models import ReportingRefreshState

        ReportingRefreshState.__table__.drop(connection)
        os.environ["APP_DB_ROLE"] = connection.execute(text("SELECT current_user")).scalar()
        migration = import_module(
            "infrastructure.persistence.alembic.versions.20261003_01_reporting_recovery"
        )
        with Operations.context(MigrationContext.configure(connection)):
            migration.upgrade()
    print(
        json.dumps(
            {
                "phase": "seeded",
                "historical_events": args.history,
                "historical_choices": args.history * 3,
            }
        ),
        flush=True,
    )

    def raw_event(source_id):
        return dict(
            id=source_id,
            startTimestamp=int(datetime(2026, 10, 2, 12, tzinfo=timezone.utc).timestamp()),
            slug=f"event-{source_id}",
            homeTeam=dict(id=1, name="Home", gender="M"),
            awayTeam=dict(id=2, name="Away", gender="M"),
            tournament=dict(
                id=50,
                name="League",
                uniqueTournament=dict(id=5, name="League"),
                category=dict(name="Country", sport=dict(name="Football", id=1)),
            ),
            status=dict(type="notstarted", code=0),
        )

    class Provider:
        from modules.sofascore.event_normalizer import normalize_event_payload
        from modules.sofascore.client import SofaScoreAPI

        normalize_event_payload = staticmethod(normalize_event_payload)
        open_scheduled_tournaments = SofaScoreAPI.open_scheduled_tournaments
        open_scheduled_events = SofaScoreAPI.open_scheduled_events
        open_scheduled_odds = SofaScoreAPI.open_scheduled_odds

        def download_json(self, endpoint, body_file, params=None):
            if "scheduled-tournaments" in endpoint:
                body_file.write(
                    b'{"scheduled":[{"tournament":{"id":50,"uniqueTournament":{"id":5}}}],"hasNextPage":false}'
                )
            elif "/odds/" in endpoint:
                body_file.write(b'{"odds":{}}')
            else:
                body_file.write(b'{"events":[')
                for index in range(args.events):
                    if index:
                        body_file.write(b",")
                    body_file.write(json.dumps(raw_event(args.history + index + 1)).encode())
                body_file.write(b"]}")
            body_file.seek(0)

    def result_response(_client, source_id, **_):
        sleep(0.001)  # Network fixture; it never reaches an external provider.
        raw = raw_event(source_id)
        raw.update(
            status=dict(type="finished", code=100),
            homeScore=dict(current=2),
            awayScore=dict(current=0),
            winnerCode=1,
        )
        return {"event": raw}

    Config.SUPPORTED_SPORTS = ["Football"]
    pipeline.load_tracked_source_competitions = lambda _: None
    batch_processor.fetch_authoritative_event_response = result_response
    DailyDiscoveryRepository.initialize_sports_for_slot("2026-10-03", "current_utc_day", ["football"])
    stop, finished = Event(), Event()
    samples, read_seconds, dispatch_seconds, outcomes = [], [], [], {}

    def sample():
        while not stop.wait(0.1):
            samples.append(dict(rss_mb=get_rss_mb(), cgroup=get_cgroup_memory_snapshot()))

    sampler = Thread(target=sample, daemon=True)
    sampler.start()
    maintenance = SerialExecutor("load-maintenance", 2)
    pre_start = SerialExecutor("load-pre-start", 2, coalesce_active=True)
    closing = SerialExecutor("load-closing", 2)

    def heavy():
        try:
            outcomes["daily"] = pipeline.discover_events_for_date(
                "2026-10-03", ["football"], "current_utc_day", client=Provider()
            )
            outcomes["results"] = run_results_collection(date(2026, 10, 2))
            outcomes["view_refresh"] = run_view_refresh(force=True)
        except Exception as exc:
            outcomes["error"] = repr(exc)
        finally:
            finished.set()

    def read(admitted):
        dispatch_seconds.append(monotonic() - admitted)
        start = monotonic()
        with database.engine.connect() as connection:
            for name in ("mv_alert_events", "mv_p5_price_memory"):
                connection.execute(
                    text(f"SELECT event_id FROM {name} ORDER BY event_id LIMIT 10")
                ).all()
        read_seconds.append(monotonic() - start)

    def ticks():
        while not stop.wait(0.2):
            admitted = monotonic()
            for executor, priority, key in (
                (pre_start, Priority.PRE_START, "read-pre-start"),
                (closing, Priority.CLOSING, "read-closing"),
            ):
                executor.submit(
                    JobRequest(key, lambda time=admitted: read(time), utc_now(), priority)
                )

    ticker = Thread(target=ticks, daemon=True)
    ticker.start()
    start = monotonic()
    maintenance.submit(JobRequest("bulk-load", heavy, utc_now(), Priority.MAINTENANCE))
    try:
        if not finished.wait(600):
            raise RuntimeError("Load deadline exceeded")
    finally:
        stop.set()
        ticker.join()
        sampler.join()
        for executor in (maintenance, pre_start, closing):
            executor.shutdown()
    outcomes.update(
        duration_s=round(monotonic() - start, 3),
        reader_runs=len(read_seconds),
        max_dispatch_s=round(max(dispatch_seconds, default=0), 4),
        max_view_read_s=round(max(read_seconds, default=0), 4),
        sampled_peak_rss_mb=max((row["rss_mb"] or 0 for row in samples), default=0),
        cgroup=get_cgroup_memory_snapshot(),
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(outcomes, indent=2), encoding="utf-8")
    print(json.dumps(outcomes), flush=True)
    if (
        outcomes.get("error")
        or outcomes["daily"]["events_persisted"] != args.events
        or outcomes["results"]["updated"] != args.events
    ):
        raise RuntimeError("Load did not persist the requested fixture")
    if outcomes["view_refresh"]["failed"] or outcomes["max_dispatch_s"] >= 1:
        raise RuntimeError("Reporting or critical dispatch failed its load target")


if __name__ == "__main__":
    main()
