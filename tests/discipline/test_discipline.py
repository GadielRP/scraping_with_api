"""Focused acceptance: supplied examples, bounded parsing and snapshot semantics."""
from contextlib import contextmanager
from dataclasses import replace
from datetime import date
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import event as sql_event, select

from infrastructure.persistence.database import DatabaseManager
from infrastructure.persistence.models import (
    Competition, DisciplinaryIncident, Event, EventSourceMapping, Participant, Result, Season,
)
from infrastructure.persistence.orm_base import Base
from infrastructure.persistence.repositories.disciplinary_incident_repository import DisciplinaryIncidentRepository
NBA, WNBA = 176, 145  # Arbitrary local database keys for these fixtures.
from modules.discipline.contracts import (
    Acquisition, DisciplineEndpoint, DisciplineEventContext, DisciplineTotalDraft, IncidentDraft, IncidentType, Origin, Outcome, Selection, Side,
)
from modules.discipline.providers.sofascore import SofaScoreDisciplineProvider
from modules.discipline.service import DisciplineService
from infrastructure.settings.discipline import DisciplineSettings
from shared.execution_context import WorkDeferred
from shared.temporal import utc_now
from modules.discipline.providers.sofascore_statistics import SofaScoreDisciplineStatisticsProvider
from infrastructure.persistence.models import DisciplinaryIncidentTotal
from infrastructure.network.json_document import JsonDocumentLimitExceeded
from modules.sofascore.exceptions import SofaScoreNotFoundException

FIXTURES = Path(__file__).parent / "fixtures"


class Client:
    def __init__(self, body):
        self.body = body if isinstance(body, bytes) else json.dumps(body).encode()

    @contextmanager
    def open_event_statistics(self, event_id):
        with io.BytesIO(b'{"statistics": []}') as document:
            yield document

    @contextmanager
    def open_event_comments(self, event_id):
        self.requested = event_id
        with io.BytesIO(self.body) as document:
            yield document


def context(sport="Football", competition_id=None):
    return DisciplineEventContext(1, 1, "16421093", "sofascore", sport, competition_id, None, None, None)


def write(repo, context, incidents, managed, **kwargs):
    return repo.replace_acquisition(context, Acquisition(Outcome.READY, incidents, managed), **kwargs)


def acquire(data, ctx=None, settings=None):
    return SofaScoreDisciplineProvider(Client(data), settings).acquire(ctx or context())


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def test_examples_and_distinct_yellows_preserve_multiplicity():
    football = fixture("football.json")
    result = acquire(football)
    assert result.outcome == Outcome.READY
    assert result.comments_count == len(football["comments"])
    assert sum(i.type == IncidentType.YELLOW_CARD and i.side == Side.HOME for i in result.incidents) == 2
    assert sum(i.type == IncidentType.YELLOW_CARD and i.side == Side.AWAY for i in result.incidents) == 3
    assert sum(i.type == IncidentType.RED_CARD for i in result.incidents) == 1
    yellow = next(c for c in football["comments"] if c["type"] == "yellowCard")
    football["comments"].append(dict(yellow, id=999))
    assert len(acquire(football).incidents) == 7
    football["comments"].append(dict(yellow))
    assert len(acquire(football).incidents) == 7  # same external comment, not a second card

    nba = fixture("nba.json")
    result = acquire(nba, context("Basketball", NBA))
    assert {(i.type, i.origin, i.source_player_id) for i in result.incidents} == {
        (IncidentType.EJECTION, Origin.REPORTED, "817122"),
        (IncidentType.FOUL_OUT, Origin.DERIVED, "817000"),
    }
    assert result.managed_types == {IncidentType.EJECTION, IncidentType.FOUL_OUT}
    nba["comments"].reverse()
    assert set(acquire(nba, context("Basketball", NBA)).incidents) == set(result.incidents)
    other = acquire(nba, context("Basketball", WNBA))
    assert [i.type for i in other.incidents] == [IncidentType.EJECTION]
    assert other.managed_types == {IncidentType.EJECTION}


@pytest.mark.parametrize("change", ["unknown", "seventh_foul", "eligibility"])
def test_unreliable_foul_counts_preserve_reported_ejection_and_prior_derived_scope(change):
    nba = fixture("nba.json")
    if change == "unknown":
        nba["comments"].append(dict(type="foul", text="Unclassified infraction"))
    elif change == "eligibility":
        nba["comments"].append(dict(type="stoppage", text="No eligible substitutes"))
    else:
        foul = next(c for c in nba["comments"] if c.get("player", {}).get("id") == 817000 and c["type"] == "foul")
        nba["comments"].append(dict(foul, id=999))
    result = acquire(nba, context("Basketball", NBA))
    assert result.outcome == Outcome.READY
    assert result.managed_types == {IncidentType.EJECTION}
    assert [i.type for i in result.incidents] == [IncidentType.EJECTION]


@pytest.mark.parametrize("body,outcome", [
    (b'{"comments": [', Outcome.INVALID),
    ({}, Outcome.INVALID),
    ({"comments": {}}, Outcome.INVALID),
    ({"comments": []}, Outcome.UNAVAILABLE),
    ({"comments": [dict(type="shotBlocked")]}, Outcome.UNAVAILABLE),
])
def test_unusable_documents_never_publish_a_snapshot(body, outcome):
    assert acquire(body).outcome == outcome


def test_pagination_invalid_side_and_limits_are_not_partial_snapshots():
    data = fixture("football.json")
    assert acquire(dict(data, hasNextPage=True)).outcome == Outcome.UNAVAILABLE
    assert acquire(data, settings=DisciplineSettings(max_incidents=1)).outcome == Outcome.DEFERRED
    next(c for c in data["comments"] if c["type"] == "yellowCard").pop("isHome")
    assert acquire(data).outcome == Outcome.INVALID


@pytest.fixture
def database(tmp_path, monkeypatch):
    # Explicit opt-in must point at a disposable database, never Config.DATABASE_URL.
    db = DatabaseManager(os.getenv("DISCIPLINE_TEST_DATABASE_URL") or f"sqlite:///{tmp_path / 'discipline.db'}")
    if db.engine.dialect.name == "sqlite":
        @sql_event.listens_for(db.engine, "connect")
        def foreign_keys(connection, _):
            connection.execute("PRAGMA foreign_keys=ON")
        Base.metadata.create_all(db.engine)
    else:
        from alembic import command
        from alembic.config import Config as AlembicConfig
        from infrastructure.settings import Config

        Base.metadata.create_all(db.engine, tables=[t for t in Base.metadata.sorted_tables
                                                    if t.name not in ("disciplinary_incidents", "disciplinary_incidents_totals")])
        monkeypatch.setattr(Config, "DATABASE_URL", db.database_url)
        config = AlembicConfig("alembic.ini")
        command.stamp(config, "20261029_01", purge=True)
        command.upgrade(config, "head")
    with db.get_session() as session:
        session.add(Competition(competition_id=NBA, source="sofascore", source_tournament_id=132,
                                canonical_name="NBA", display_name="NBA"))
        session.add(Season(id=1, name="2026/27"))
        session.add_all([Participant(participant_id=i, source="sofascore", source_participant_id=i, name=str(i)) for i in (1, 2)])
        session.flush()
        session.add(Event(id=1, slug="game", sport="Basketball", starts_at=utc_now(), competition="NBA",
                          home_team="Home", away_team="Away", competition_id=NBA, season_id=1))
        session.flush()
        session.add(Result(event_id=1, home_score=1, away_score=0))
        session.add(EventSourceMapping(event_id=1, source="sofascore", source_event_id="457265",
                                       source_participant_home_id=1, source_participant_away_id=2))
    yield db
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()


def test_repository_idempotence_multiplicity_scope_identity_and_team_totals(database):
    repo = DisciplinaryIncidentRepository(database)
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    assert ctx.source_event_id == "457265"
    yellow = IncidentDraft(IncidentType.YELLOW_CARD, Origin.REPORTED, "9999", "Player", Side.HOME)
    red = replace(yellow, type=IncidentType.RED_CARD, side=Side.AWAY)
    scope = frozenset((IncidentType.YELLOW_CARD, IncidentType.RED_CARD))
    assert write(repo, ctx, (yellow, yellow, red), scope, dry_run=True) == Outcome.UPDATED
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 0
    assert write(repo, ctx, (yellow, yellow, red), scope) == Outcome.UPDATED
    with database.get_session() as session:
        before = session.execute(select(DisciplinaryIncident.id, DisciplinaryIncident.created_at)).all()
    assert write(repo, ctx, (red, yellow, yellow), scope) == Outcome.UNCHANGED
    with database.get_session() as session:
        assert session.execute(select(DisciplinaryIncident.id, DisciplinaryIncident.created_at)).all() == before
    assert repo.get_team_season_totals("sofascore", 1, NBA, 1).total_cards == 2
    assert repo.get_team_season_totals("sofascore", 2, NBA, 1).red_cards == 1
    assert repo.get_team_season_totals("other", 1, NBA, 1).total_cards == 0
    assert write(repo, ctx, (yellow,), frozenset((IncidentType.YELLOW_CARD,))) == Outcome.UPDATED
    assert repo.get_team_season_totals("sofascore", 2, NBA, 1).red_cards == 1
    with database.get_session() as session:
        session.get(EventSourceMapping, 1).source_event_id = "changed"
    assert write(repo, ctx, (), scope) == Outcome.MAPPING_CONFLICT
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 2
        session.query(EventSourceMapping).delete(synchronize_session=False)
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 0
    assert write(repo, ctx, (), scope) == Outcome.MAPPING_CONFLICT


def test_service_closes_read_session_before_http_and_dry_run_never_writes(database):
    repo = DisciplinaryIncidentRepository(database)
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    @contextmanager
    def exclusion(event_id):
        assert event_id == 1
        yield True
    class CheckedClient(Client):
        def open_event_comments(self, sid):
            # An exclusion connection is intentionally not used in this test.
            assert database.engine.pool.checkedout() == 0
            return super().open_event_comments(sid)
    provider = SofaScoreDisciplineProvider(CheckedClient(fixture("nba.json")))
    service = DisciplineService(provider, repo, exclusion)
    assert service.process(ctx).outcome == Outcome.UPDATED
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 0
    assert service.process(ctx, commit=True).outcome == Outcome.UPDATED
    assert service.process(ctx, commit=True).outcome == Outcome.UNCHANGED
    @contextmanager
    def busy(_):
        yield False
    assert DisciplineService(provider, repo, busy).process(ctx).outcome == Outcome.DEFERRED


def test_job_limit_cursor_and_work_deferred(database):
    from modules.jobs.discipline_collection_job import run_discipline_collection
    repo = DisciplinaryIncidentRepository(database)
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    events = SimpleNamespace(iter_candidates=lambda *_: iter([[ctx, replace(ctx, event_id=2)]]))
    service = SimpleNamespace(process=lambda *a, **kw: SimpleNamespace(outcome=Outcome.UNCHANGED, acquisition=None))
    result = run_discipline_collection(service, events, DisciplineSettings(), selection=Selection(event_id=1), limit=1)
    assert result["limit_reached"] and result["scanned"] == 1 and result["last_processed_event_id"] == 1
    def deferred(*args, **kwargs):
        raise WorkDeferred("deadline")
    service.process = deferred
    with pytest.raises(WorkDeferred):
        run_discipline_collection(service, events, DisciplineSettings(), selection=Selection(event_id=1))


@pytest.mark.parametrize(
    ("tracked_only", "expected_event_ids", "expected_skipped"),
    [
        (True, [1], 1),
        (False, [1, 2], 0),
    ],
)
def test_discipline_job_respects_tracked_competitions_setting(
    tracked_only, expected_event_ids, expected_skipped
):
    from modules.jobs.discipline_collection_job import run_discipline_collection

    tracked = context("Basketball", NBA)
    untracked = replace(tracked, event_id=2, competition_id=WNBA)
    repository = SimpleNamespace(
        iter_candidates=lambda *_: iter([[tracked, untracked]])
    )
    processed_event_ids = []
    service = SimpleNamespace(
        process=lambda event, **_: (
            processed_event_ids.append(event.event_id)
            or SimpleNamespace(outcome=Outcome.UNCHANGED, acquisition=None)
        )
    )

    summary = run_discipline_collection(
        service,
        repository,
        DisciplineSettings(tracked_competitions_only=tracked_only),
        selection=Selection(event_ids=(1, 2)),
    )

    assert processed_event_ids == expected_event_ids
    assert summary["skipped_untracked"] == expected_skipped


@pytest.mark.parametrize("origin", ["midnight", "intraday"])
@pytest.mark.parametrize("tracked_only", [True, False])
def test_post_result_collector_filters_without_querying_and_respects_toggle(monkeypatch, origin, tracked_only):
    from modules.jobs.discipline_collection_job import post_results, composition
    from modules.jobs.results_collection_job.contracts import CommittedResultEvent

    calls = []
    monkeypatch.setattr(composition, "build_discipline_runner", lambda _: lambda **kw: calls.append(kw))
    monkeypatch.setattr(post_results, "DisciplineSettings", lambda: DisciplineSettings(
        after_midnight_results_enabled=False, after_intraday_results_enabled=False))
    assert post_results.build_post_results_collector(origin=origin) is None
    monkeypatch.setattr(post_results, "DisciplineSettings", lambda: DisciplineSettings(
        after_midnight_results_enabled=True, after_intraday_results_enabled=True,
        full_feed_verified=True, tracked_competitions_only=tracked_only))
    collect = post_results.build_post_results_collector(origin=origin)
    collect((CommittedResultEvent(1, "Basketball", NBA),
             CommittedResultEvent(2, "Basketball", -1),
             CommittedResultEvent(3, "Baseball", NBA)))
    assert len(calls) == 1
    assert calls[0]["selection"].event_ids == ((1,) if tracked_only else (1, 2))
    assert calls[0]["commit"] and calls[0]["limit"] == (1 if tracked_only else 2)


def test_intraday_hands_off_confirmed_ids_with_existing_metadata_after_commit(monkeypatch):
    from modules.jobs.pre_start_check_job import intraday_result_freshness as intraday
    from modules.jobs.results_collection_job.contracts import CommittedResultEvent

    events = [dict(id=i, sport="Basketball", competition_id=NBA) for i in (1, 2)]
    committed = []
    seen = []
    monkeypatch.setattr(intraday, "_should_check_result_now", lambda _: True)
    monkeypatch.setattr(intraday, "bounded_results", lambda *args: iter([
        {"upsert_data": (1, {"home_score": 1, "away_score": 0})},
        {"upsert_data": (2, {"home_score": 2, "away_score": 0})},
    ]))
    def persist(rows):
        committed.append(True)
        return {1}  # Event 2 was not confirmed by persistence.
    def collect(batch):
        assert committed
        seen.extend(batch)
    def build(**kwargs):
        assert kwargs == {"origin": "intraday"}
        return collect
    monkeypatch.setattr(intraday.ResultRepository, "batch_upsert_results", persist)
    monkeypatch.setattr(intraday, "build_post_results_collector", build)
    assert intraday.process_intraday_result_freshness(events)["results_upserted"] == 1
    assert seen == [CommittedResultEvent(1, "Basketball", NBA)]


def test_explicit_ids_use_one_read_query_and_only_select_requested_events(database):
    repo = DisciplinaryIncidentRepository(database)
    reads = []
    def record(conn, cursor, statement, *args):
        if statement.startswith("SELECT"):
            reads.append(statement)
    sql_event.listen(database.engine, "before_cursor_execute", record)
    try:
        assert next(repo.iter_candidates(Selection(event_ids=(1,)), 100))[0].event_id == 1
        assert len(reads) == 1
    finally:
        sql_event.remove(database.engine, "before_cursor_execute", record)


def test_cli_selectors_and_schedule_occurrence():
    from app.cli import _build_parser, _discipline_selection
    from app.runtime import ApplicationRuntime
    from infrastructure.scheduler.schedules import configure_calendar
    from infrastructure.settings.job_execution import JobExecutionSettings
    from shared.execution_context import Priority
    import schedule

    parser = _build_parser()
    args = parser.parse_args(["discipline", "--event-id", "12345"])
    assert not args.commit and _discipline_selection(args).event_id == 12345
    args = parser.parse_args(["discipline", "--event-id", "1", "--date", "2026-11-03"])
    with pytest.raises(ValueError):
        _discipline_selection(args)
    clock = schedule.Scheduler()
    seen = []
    configure_calendar(clock, JobExecutionSettings(), lambda name, *_: seen.append(name))
    for job in clock.jobs:
        job.run()
    assert "discipline" not in seen
    runtime = ApplicationRuntime.__new__(ApplicationRuntime)
    runtime.discipline_settings = DisciplineSettings(full_feed_verified=False)
    with pytest.raises(ValueError, match="full-match"):
        runtime._run_discipline(commit=True)


def test_rollback_and_explicit_event_deletion(database):
    from infrastructure.persistence.repositories.event_deletion_writer import delete_events
    repo = DisciplinaryIncidentRepository(database)
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    snapshot = acquire(fixture("nba.json"), ctx)
    write(repo, ctx, snapshot.incidents, snapshot.managed_types)
    def fail_insert(connection, cursor, statement, *args):
        if statement.startswith("INSERT INTO disciplinary_incidents"):
            raise RuntimeError("injected insert failure")
    sql_event.listen(database.engine, "before_cursor_execute", fail_insert)
    try:
        changed = (replace(snapshot.incidents[0], player_name="Corrected"),)
        with pytest.raises(RuntimeError, match="injected"):
            write(repo, ctx, changed, snapshot.managed_types)
    finally:
        sql_event.remove(database.engine, "before_cursor_execute", fail_insert)
    assert write(repo, ctx, snapshot.incidents, snapshot.managed_types) == Outcome.UNCHANGED
    assert delete_events(database, [ctx.event_id]) == 1
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 0


def test_postgres_migration_constraints_and_exclusion(database, monkeypatch):
    if database.engine.dialect.name != "postgresql":
        pytest.skip("Set DISCIPLINE_TEST_DATABASE_URL to a disposable PostgreSQL database")
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event as Signal
    from functools import partial
    from sqlalchemy import inspect
    from sqlalchemy.exc import IntegrityError
    from infrastructure.persistence import advisory_lock

    inspector = inspect(database.engine)
    assert len(inspector.get_check_constraints("disciplinary_incidents")) == 5
    assert inspector.get_foreign_keys("disciplinary_incidents")[0]["options"]["ondelete"] == "CASCADE"
    columns = {c["name"]: c for c in inspector.get_columns("disciplinary_incidents")}
    assert columns["created_at"]["type"].timezone
    assert str(columns["id"]["type"]) == "BIGINT"
    assert inspector.get_pk_constraint("disciplinary_incidents_totals")["constrained_columns"] == ["event_source_mapping_id", "type", "side"]
    assert len(inspector.get_check_constraints("disciplinary_incidents_totals")) == 4
    assert inspector.get_foreign_keys("disciplinary_incidents_totals")[0]["options"]["ondelete"] == "CASCADE"
    total_columns = {c["name"]: c for c in inspector.get_columns("disciplinary_incidents_totals")}
    assert total_columns["updated_at"]["type"].timezone
    with pytest.raises(IntegrityError, match="ck_discipline_totals_count"), database.get_session() as session:
        session.add(DisciplinaryIncidentTotal(event_source_mapping_id=1, type="yellow_card",
                                             side="home", count=-1, endpoint="statistics"))
    with pytest.raises(IntegrityError), database.get_session() as session:
        session.add(DisciplinaryIncident(event_source_mapping_id=1, type="wrong", origin="reported", side="home"))
    monkeypatch.setattr(advisory_lock, "db_manager", database)
    repo = DisciplinaryIncidentRepository(database)
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    entered, release = Signal(), Signal()
    class SlowClient(Client):
        @contextmanager
        def open_event_comments(self, sid):
            entered.set()
            assert release.wait(5)
            with super().open_event_comments(sid) as document:
                yield document
    provider = SofaScoreDisciplineProvider(SlowClient(fixture("nba.json")))
    service = DisciplineService(provider, repo, partial(advisory_lock.exclusive_slot, 736106))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(service.process, ctx, commit=True)
        try:
            assert entered.wait(3)
            assert service.process(ctx, commit=True).outcome == Outcome.DEFERRED
        finally:
            release.set()
        assert future.result(timeout=5).outcome == Outcome.UPDATED
    # Deletion and reconciliation share identity -> parent lock ordering.
    from sqlalchemy import delete
    from infrastructure.persistence.event_identity_lock import lock_event_identities
    with ThreadPoolExecutor(max_workers=1) as pool:
        with database.get_session() as session:
            lock_event_identities(session, [(ctx.source, ctx.source_event_id)])
            session.execute(select(Event.id).where(Event.id == ctx.event_id).with_for_update()).one()
            future = pool.submit(service.process, ctx, commit=True)
            session.execute(delete(Event).where(Event.id == ctx.event_id))
        assert future.result(timeout=5).outcome == Outcome.MAPPING_CONFLICT
    with database.get_session() as session:
        assert session.query(DisciplinaryIncident).count() == 0
    # Downgrade only this disposable database, never production data.
    from alembic import command
    from alembic.config import Config as AlembicConfig
    command.downgrade(AlembicConfig("alembic.ini"), "20261104_01")
    assert "disciplinary_incidents_totals" not in inspect(database.engine).get_table_names()
    assert "disciplinary_incidents" in inspect(database.engine).get_table_names()
    command.downgrade(AlembicConfig("alembic.ini"), "20261029_01")
    assert "disciplinary_incidents" not in inspect(database.engine).get_table_names()
    assert "disciplinary_incidents_totals" not in inspect(database.engine).get_table_names()


def test_keyset_has_one_row_per_event_reports_ambiguity_and_keeps_high_water(database):
    repo = DisciplinaryIncidentRepository(database)
    with database.get_session() as session:
        for eid in (2, 3):
            session.add(Event(id=eid, slug=str(eid), sport="Basketball", starts_at=utc_now(),
                              competition="NBA", competition_id=NBA, season_id=1, home_team="A", away_team="B"))
        session.flush()
        session.add_all([Result(event_id=eid, home_score=0, away_score=0) for eid in (2, 3)])
        session.add_all([EventSourceMapping(event_id=2, source="sofascore", source_event_id=sid) for sid in ("2", "22")])
        session.add(EventSourceMapping(event_id=1, source="other", source_event_id="foreign"))
    selection = Selection(competition_id=NBA, season_id=1)
    pages = repo.iter_candidates(selection, 1)
    assert [c.event_id for c in next(pages)] == [1]
    # A later insert is outside the high-water mark already captured by this run.
    with database.get_session() as session:
        session.add(Event(id=4, slug="4", sport="Basketball", starts_at=utc_now(), competition="NBA",
                          competition_id=NBA, season_id=1, home_team="A", away_team="B"))
        session.flush()
        session.add(Result(event_id=4, home_score=0, away_score=0))
    ambiguous, missing = next(pages)[0], next(pages)[0]
    assert (ambiguous.event_id, ambiguous.mapping_count, ambiguous.mapping_id) == (2, 2, None)
    assert (missing.event_id, missing.mapping_count, missing.mapping_id) == (3, 0, None)
    assert list(pages) == []


class DualClient(Client):
    def __init__(self, comments, statistics):
        super().__init__(comments if not isinstance(comments, Exception) else {})
        self.comments_error = comments if isinstance(comments, Exception) else None
        self.statistics = statistics
        self.statistics_calls = 0

    @contextmanager
    def open_event_comments(self, sid):
        if self.comments_error:
            raise self.comments_error
        with super().open_event_comments(sid) as document:
            yield document

    @contextmanager
    def open_event_statistics(self, sid):
        self.statistics_calls += 1
        if isinstance(self.statistics, Exception):
            raise self.statistics
        body = self.statistics if isinstance(self.statistics, bytes) else json.dumps(self.statistics).encode()
        with io.BytesIO(body) as document:
            yield document


@pytest.mark.parametrize("comments", [
    SofaScoreNotFoundException(1, "/event/1/comments"),
    {"comments": []}, b'{"comments":[',
    {"comments": [{"type": "matchStarted"}, {"type": "matchEnded"}]},
    RuntimeError("transport failed"),
])
def test_statistics_fallback_without_inventing_recipients(comments):
    client = DualClient(comments, fixture("football_statistics.json"))
    result = SofaScoreDisciplineProvider(client).acquire(context())
    assert result.outcome == Outcome.READY and result.statistics_attempted
    assert not result.incidents and not result.managed_types
    assert {(t.type, t.side): t.count for t in result.totals} == {
        (IncidentType.YELLOW_CARD, Side.HOME): 2, (IncidentType.YELLOW_CARD, Side.AWAY): 3,
        (IncidentType.RED_CARD, Side.HOME): 0, (IncidentType.RED_CARD, Side.AWAY): 1,
    }
    assert all(t.endpoint == DisciplineEndpoint.STATISTICS for t in result.totals)


def test_statistics_toggle_precedence_and_no_basketball_request():
    statistics = fixture("football_statistics.json")
    statistics["statistics"][0]["groups"][0]["statisticsItems"][0]["homeValue"] = 4
    client = DualClient(fixture("football.json"), statistics)
    comments = SofaScoreDisciplineProvider(client).acquire(context())
    assert client.statistics_calls == 0
    assert next(t.count for t in comments.totals if t.type == IncidentType.YELLOW_CARD and t.side == Side.HOME) == 2
    result = SofaScoreDisciplineProvider(client, DisciplineSettings(statistics_for_all_events=True)).acquire(context())
    assert client.statistics_calls == 1 and len(result.incidents) == 6
    assert next(t.count for t in result.totals if t.type == IncidentType.YELLOW_CARD and t.side == Side.HOME) == 4
    nba = DualClient(fixture("nba.json"), statistics)
    assert SofaScoreDisciplineProvider(nba, DisciplineSettings(statistics_for_all_events=True)).acquire(context("Basketball", NBA)).incidents
    assert nba.statistics_calls == 0


@pytest.mark.parametrize("error", [WorkDeferred("deadline"), JsonDocumentLimitExceeded("budget"), MemoryError()])
def test_resource_exhaustion_never_starts_statistics(error):
    client = DualClient(error, fixture("football_statistics.json"))
    if isinstance(error, JsonDocumentLimitExceeded):
        assert SofaScoreDisciplineProvider(client).acquire(context()).outcome == Outcome.DEFERRED
    else:
        with pytest.raises(type(error)):
            SofaScoreDisciplineProvider(client).acquire(context())
    assert client.statistics_calls == 0


@pytest.mark.parametrize("value", [True, -1, 1.5, "2", None])
def test_statistics_invalid_pair_does_not_replace_that_type(value):
    data = fixture("football_statistics.json")
    data["statistics"][0]["groups"][0]["statisticsItems"][0]["homeValue"] = value
    result = SofaScoreDisciplineStatisticsProvider(DualClient({}, data)).acquire(context())
    assert result.outcome == Outcome.READY
    assert {t.type for t in result.totals} == {IncidentType.RED_CARD}


def test_statistics_truncation_conflicts_missing_all_and_explicit_zero():
    data = fixture("football_statistics.json")
    provider = lambda body: SofaScoreDisciplineStatisticsProvider(DualClient({}, body)).acquire(context())
    assert provider(json.dumps(data).encode()[:-1]).outcome == Outcome.INVALID
    assert provider({"statistics": data["statistics"][1:]}).outcome == Outcome.UNAVAILABLE
    group = data["statistics"][0]["groups"][0]["statisticsItems"]
    group.append(dict(group[0], homeValue=9))
    assert provider(data).outcome == Outcome.INVALID
    group.pop()
    for item in group:
        item["homeValue"] = item["awayValue"] = 0
    result = provider(data)
    assert result.outcome == Outcome.READY and len(result.totals) == 4
    assert all(t.count == 0 for t in result.totals)


def test_totals_override_detail_without_deleting_it_and_restore_comments(database):
    repo = DisciplinaryIncidentRepository(database)
    with database.get_session() as session:
        session.get(Event, 1).sport = "Football"
    ctx = next(repo.iter_candidates(Selection(event_id=1), 100))[0]
    comments = SofaScoreDisciplineProvider(DualClient(fixture("football.json"), {})).acquire(ctx)
    assert repo.replace_acquisition(ctx, comments, dry_run=True) == Outcome.UPDATED
    with database.get_session() as session:
        assert session.query(DisciplinaryIncidentTotal).count() == 0
    assert repo.replace_acquisition(ctx, comments) == Outcome.UPDATED
    with database.get_session() as session:
        before = session.execute(select(DisciplinaryIncidentTotal.created_at, DisciplinaryIncidentTotal.updated_at)).all()
        detail_ids = session.scalars(select(DisciplinaryIncident.id)).all()
    assert repo.replace_acquisition(ctx, comments) == Outcome.UNCHANGED
    with database.get_session() as session:
        assert session.execute(select(DisciplinaryIncidentTotal.created_at, DisciplinaryIncidentTotal.updated_at)).all() == before
    stats = Acquisition(Outcome.READY, totals=(DisciplineTotalDraft(IncidentType.YELLOW_CARD, Side.HOME, 0, DisciplineEndpoint.STATISTICS),))
    assert repo.replace_acquisition(ctx, stats) == Outcome.UPDATED
    assert repo.get_team_season_totals("sofascore", 1, NBA, 1).yellow_cards == 0
    assert repo.get_team_season_totals("sofascore", 2, NBA, 1).total_cards == 4
    assert repo.get_team_season_totals("other", 1, NBA, 1).total_cards == 0
    assert any(t.endpoint == DisciplineEndpoint.STATISTICS for t in repo.get_event_totals("sofascore", 1))
    with database.get_session() as session:
        assert session.scalars(select(DisciplinaryIncident.id)).all() == detail_ids
    assert repo.replace_acquisition(ctx, comments) == Outcome.UPDATED
    assert repo.get_team_season_totals("sofascore", 1, NBA, 1).yellow_cards == 2
    # Totals-only writes are also rolled back if the transaction fails.
    def fail(conn, cursor, statement, *args):
        if statement.startswith("INSERT INTO disciplinary_incidents_totals"):
            raise RuntimeError("totals failure")
    sql_event.listen(database.engine, "before_cursor_execute", fail)
    try:
        with pytest.raises(RuntimeError, match="totals failure"):
            repo.replace_acquisition(ctx, replace(comments, incidents=comments.incidents[:1], totals=stats.totals))
    finally:
        sql_event.remove(database.engine, "before_cursor_execute", fail)
    assert repo.replace_acquisition(ctx, comments) == Outcome.UNCHANGED
    with database.get_session() as session:
        session.query(EventSourceMapping).delete(synchronize_session=False)
    with database.get_session() as session:
        assert session.query(DisciplinaryIncidentTotal).count() == 0


def test_failed_fallback_preserves_prior_data(database):
    repo = DisciplinaryIncidentRepository(database)
    with database.get_session() as session:
        session.get(Event, 1).sport = "Football"
    ctx = next(repo.iter_candidates(Selection(event_id=1), 1))[0]
    previous = SofaScoreDisciplineProvider(DualClient(fixture("football.json"), {})).acquire(ctx)
    repo.replace_acquisition(ctx, previous)
    @contextmanager
    def slot(_):
        yield True
    provider = SofaScoreDisciplineProvider(DualClient(
        {"comments": [{"type": "matchStarted"}, {"type": "matchEnded"}]}, {"statistics": []}))
    assert DisciplineService(provider, repo, slot).process(ctx, commit=True).outcome == Outcome.UNAVAILABLE
    assert repo.replace_acquisition(ctx, previous) == Outcome.UNCHANGED
