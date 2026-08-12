from __future__ import annotations

from decimal import Decimal

from infrastructure.persistence.repositories.odds_trajectory_repository import (
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
        collected_at=None,
        minutes_before_start=0,
        target_minute=0,
        distance_from_target=0,
    )


def test_point_serialization_keeps_quote_identity():
    payload = _point(99).to_dict()
    assert payload["quote_id"] == 99
    assert payload["source"] == "sofascore"
    assert payload["exchange_side"] is None
    assert payload["exchange_level"] == 0


class _Rows:
    def mappings(self):
        return self

    def all(self):
        return []


class _Session:
    statement = None

    def execute(self, statement, _params):
        self.statement = str(statement)
        return _Rows()


class _Context:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *_args):
        return False


def test_quote_ranking_partitions_by_quote_not_choice(monkeypatch):
    session = _Session()
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.odds_trajectory_repository.db_manager.get_session",
        lambda: _Context(session),
    )

    result = OddsTrajectoryRepository._load_pre_start_trajectory_map(
        event_ids=[1],
        target_minutes=[120, 0],
        tolerance_minutes=5,
    )

    assert result == {}
    assert "PARTITION BY event_id, quote_id, target_minute" in session.statement
    assert "traj.quote_id IS NOT NULL" in session.statement
    assert "FROM v_pre_start_odds_trajectory traj" in session.statement
