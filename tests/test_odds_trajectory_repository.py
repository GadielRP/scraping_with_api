from __future__ import annotations

from decimal import Decimal

import pytest

from infrastructure.persistence.repositories.odds_trajectory_repository import (
    OddsTrajectoryLoadError,
    OddsTrajectoryPoint,
    OddsTrajectoryRepository,
)


def _point(quote_id):
    return OddsTrajectoryPoint(
        event_id=1,
        market_id=2,
        canonical_market_key="1x2",
        market_family="moneyline",
        market_display_order=1,
        market_name="Result",
        market_group="1X2",
        market_period="Full Time",
        choice_group=None,
        bookie_id=1,
        bookie_name="SofaScore",
        choice_id=3,
        choice_name="1",
        choice_display_order=1,
        quote_id=quote_id,
        source="sofascore",
        exchange_side=None,
        exchange_level=0,
        initial_odds=Decimal("2"),
        odds_value=Decimal("1.9"),
        snapshot_id=4,
        source_collected_at=None,
        collected_at=None,
        observed_minutes_before_start=1,
        trajectory_minutes_before_start=Decimal("2.125"),
        source_limit=Decimal("100"),
    )


def test_point_serialization_keeps_quote_identity():
    payload = _point(99).to_dict()
    assert payload["quote_id"] == 99
    assert payload["source"] == "sofascore"
    assert payload["exchange_side"] is None
    assert payload["exchange_level"] == 0
    assert payload["exchange_size"] is None
    assert payload["source_collected_at"] is None
    assert payload["observed_minutes_before_start"] == 1
    assert payload["trajectory_minutes_before_start"] == Decimal("2.125")
    assert payload["source_limit"] == Decimal("100")


class _Rows:
    def mappings(self):
        return self

    def __iter__(self):
        return iter(())


class _Session:
    statement = None
    params = None

    def execute(self, statement, params):
        self.statement = str(statement)
        self.params = params
        return _Rows()


class _Context:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *_args):
        return False


def test_event_scope_precedes_complete_snapshot_history(monkeypatch):
    session = _Session()
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.odds_trajectory_repository.db_manager.get_session",
        lambda: _Context(session),
    )

    result = OddsTrajectoryRepository._load_pre_start_trajectory_map(
        event_ids=[1],
    )

    assert result == {}
    assert "WITH requested_events AS" in session.statement
    assert "WHERE e.id IN" in session.statement
    assert "FROM requested_events requested" in session.statement
    assert "WHERE mcq.main_line IS TRUE" in session.statement
    assert session.statement.index("JOIN markets m") < session.statement.index(
        "JOIN market_choice_snapshots snapshots"
    )
    assert "CROSS JOIN" not in session.statement
    assert "target_minute" not in session.statement
    assert "distance_from_target" not in session.statement
    assert "snapshots.exchange_size" in session.statement
    assert "snapshots.source_limit" in session.statement
    assert "observed_minutes_before_start" in session.statement
    assert "trajectory_minutes_before_start" in session.statement
    assert "COALESCE(" in session.statement
    assert session.params["event_ids"] == [1]
    assert set(session.params) == {"event_ids"}


class _FailingSession:
    def execute(self, _statement, _params):
        raise RuntimeError("database unavailable")


def test_repository_distinguishes_query_failure_from_empty_result(monkeypatch):
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.odds_trajectory_repository.db_manager.get_session",
        lambda: _Context(_FailingSession()),
    )

    with pytest.raises(OddsTrajectoryLoadError):
        OddsTrajectoryRepository._load_pre_start_trajectory_map(
            event_ids=[1],
        )


def test_public_read_normalizes_duplicate_event_ids(monkeypatch):
    session = _Session()
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.odds_trajectory_repository.db_manager.get_session",
        lambda: _Context(session),
    )

    result = OddsTrajectoryRepository.get_pre_start_trajectory_map(
        event_ids=[2, 1, 2],
    )

    assert result == {}
    assert session.params["event_ids"] == [1, 2]
    assert set(session.params) == {"event_ids"}
