from __future__ import annotations

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from infrastructure.persistence.repositories.market.market_quote_readiness import MarketQuoteReadinessAuditor


SCHEMA_SQL = (
    "CREATE TABLE markets (market_id INTEGER PRIMARY KEY, event_id INTEGER NOT NULL)",
    """CREATE TABLE market_choices (
        choice_id INTEGER PRIMARY KEY,
        market_id INTEGER NOT NULL,
        choice_name TEXT NOT NULL
    )""",
    """CREATE TABLE market_choice_quotes (
        quote_id INTEGER PRIMARY KEY,
        choice_id INTEGER NOT NULL,
        source TEXT,
        exchange_side TEXT,
        exchange_level INTEGER NOT NULL
    )""",
    """CREATE TABLE market_choice_snapshots (
        snapshot_id INTEGER PRIMARY KEY,
        quote_id INTEGER
    )""",
)


def _engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        for statement in SCHEMA_SQL:
            connection.exec_driver_sql(statement)
    return engine


def _insert_ready_fixture(engine):
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO markets VALUES (1, 100), (2, 200)"))
        connection.execute(
            text(
                "INSERT INTO market_choices VALUES "
                "(10, 1, '1'), (20, 2, '1')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO market_choice_quotes VALUES "
                "(1000, 10, 'sofascore', NULL, 0), "
                "(2000, 20, 'oddspapi', NULL, 0)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO market_choice_snapshots VALUES "
                "(10000, 1000), (20000, 2000)"
            )
        )


def test_schema_preflight_reports_missing_table_and_columns():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE markets (market_id INTEGER)")

    errors = MarketQuoteReadinessAuditor.schema_preflight(engine)

    assert "missing_column:markets.event_id" in errors
    assert "missing_table:market_choice_quotes" in errors
    assert "missing_table:market_choice_snapshots" in errors


def test_ready_fixture_passes_for_all_and_event_scope():
    engine = _engine()
    _insert_ready_fixture(engine)

    with Session(engine) as session:
        all_report = MarketQuoteReadinessAuditor.audit(session)
        scoped_report = MarketQuoteReadinessAuditor.audit(session, event_ids=[100])

    assert all_report.ready is True
    assert all_report.issues == ()
    assert scoped_report.ready is True
    assert scoped_report.event_scope == (100,)


def test_audit_reports_every_blocking_data_class_in_scope():
    engine = _engine()
    _insert_ready_fixture(engine)
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO market_choices VALUES (11, 1, '2')")
        )
        connection.execute(
            text(
                "INSERT INTO market_choice_quotes VALUES "
                "(1001, 10, '', NULL, 0), "
                "(1002, 10, 'sofascore', NULL, 0), "
                "(1003, 10, 'sofascore', 'invalid', -1)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO market_choice_snapshots VALUES "
                "(10001, NULL)"
            )
        )

    with Session(engine) as session:
        report = MarketQuoteReadinessAuditor.audit(session, event_ids=[100])

    issues = {item.code: item for item in report.issues}
    assert report.ready is False
    assert {
        "unlinked_snapshot",
        "invalid_quote_side_or_level",
        "invalid_quote_source",
        "duplicate_quote_identity",
    } <= set(issues)
    assert issues["unlinked_snapshot"].sample_ids == (10001,)
