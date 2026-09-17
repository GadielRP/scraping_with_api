from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, select

from infrastructure.persistence.migrations.event_timezones import (
    EVENT_TIMESTAMP_MIGRATIONS,
    RETIRED_EVENT_VIEWS,
    migrate_event_timezones,
)
from infrastructure.persistence.types import UTCDateTime
from modules.jobs.pre_start_check_job.timing import minutes_until_start
from shared.temporal import (
    NaiveDateTimeError,
    as_utc,
    from_unix_timestamp,
    interpret_local_naive,
    local_day_bounds_utc,
)


def test_unix_timestamp_is_an_aware_utc_instant():
    assert from_unix_timestamp(0) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_absolute_instant_rejects_naive_datetime():
    with pytest.raises(NaiveDateTimeError):
        as_utc(datetime(2026, 9, 16, 12, 0))


def test_legacy_mexico_wall_clock_is_interpreted_not_relabelled():
    assert interpret_local_naive(
        datetime(2026, 9, 16, 12, 0),
        "America/Mexico_City",
    ) == datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)


def test_local_day_bounds_are_converted_to_a_half_open_utc_window():
    assert local_day_bounds_utc(
        date(2026, 9, 16),
        "America/Mexico_City",
    ) == (
        datetime(2026, 9, 16, 6, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 17, 6, 0, tzinfo=timezone.utc),
    )


def test_local_day_bounds_follow_historical_dst_rules_instead_of_fixed_offset():
    start, end = local_day_bounds_utc(
        date(2022, 10, 30),
        "America/Mexico_City",
    )
    assert (end - start).total_seconds() == 25 * 60 * 60


def test_minutes_until_start_compares_instants_not_wall_clocks():
    mexico = ZoneInfo("America/Mexico_City")
    assert minutes_until_start(
        datetime(2026, 9, 16, 13, 0, tzinfo=mexico),
        now=datetime(2026, 9, 16, 18, 30, tzinfo=timezone.utc),
    ) == 30


def test_utc_datetime_round_trip_preserves_aware_contract_on_sqlite():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
    records = Table(
        "records",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("happened_at", UTCDateTime(), nullable=False),
    )
    metadata.create_all(engine)

    source = datetime(
        2026,
        9,
        16,
        12,
        0,
        tzinfo=ZoneInfo("America/Mexico_City"),
    )
    with engine.begin() as connection:
        connection.execute(insert(records).values(id=1, happened_at=source))
        loaded = connection.execute(select(records.c.happened_at)).scalar_one()

    assert loaded == datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)
    assert loaded.tzinfo is timezone.utc


def test_utc_datetime_rejects_naive_values_even_on_sqlite():
    utc_type = UTCDateTime()
    sqlite_dialect = create_engine("sqlite:///:memory:").dialect
    with pytest.raises(NaiveDateTimeError):
        utc_type.process_bind_param(datetime(2026, 9, 16, 12, 0), sqlite_dialect)


def test_event_migration_contract_declares_each_legacy_origin_zone():
    contracts = {
        (migration.table_name, migration.column_name): migration.legacy_timezone
        for migration in EVENT_TIMESTAMP_MIGRATIONS
    }
    assert contracts == {
        ("events", "start_time_utc"): "America/Mexico_City",
        ("event_source_resolution_queue", "source_start_time_utc"): "UTC",
    }


def test_obsolete_event_views_are_retired_explicitly():
    assert RETIRED_EVENT_VIEWS == ("basketball_results_season_year",)


def test_postgresql_only_migration_is_a_noop_on_sqlite():
    engine = create_engine("sqlite:///:memory:")
    assert migrate_event_timezones(engine) == []
