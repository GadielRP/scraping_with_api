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


def test_shadow_key_prefers_quote_id_and_falls_back_to_legacy_identity():
    quote_key = OddsTrajectoryRepository._shadow_point_key(_point(99))
    legacy_key = OddsTrajectoryRepository._shadow_point_key(_point(None))
    assert quote_key[0] == ("quote", 99)
    assert legacy_key[0][:4] == ("legacy", 2, 1, 3)


def test_shadow_sample_is_deterministic_and_honors_boundaries():
    assert OddsTrajectoryRepository._is_shadow_sampled([2, 1], 0) is False
    assert OddsTrajectoryRepository._is_shadow_sampled([2, 1], 1) is True
    assert OddsTrajectoryRepository._is_shadow_sampled([2, 1], 0.25) == (
        OddsTrajectoryRepository._is_shadow_sampled([1, 2], 0.25)
    )


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
        view_name="v_pre_start_odds_trajectory_quotes",
        quote_aware=True,
    )

    assert result == {}
    assert "PARTITION BY event_id, quote_id, target_minute" in session.statement
    assert "traj.quote_id IS NOT NULL" in session.statement
    assert "FROM v_pre_start_odds_trajectory_quotes traj" in session.statement


def test_legacy_ranking_retains_previous_identity(monkeypatch):
    session = _Session()
    monkeypatch.setattr(
        "infrastructure.persistence.repositories.odds_trajectory_repository.db_manager.get_session",
        lambda: _Context(session),
    )

    OddsTrajectoryRepository._load_pre_start_trajectory_map(
        event_ids=[1],
        target_minutes=[0],
        tolerance_minutes=5,
        view_name="v_pre_start_odds_trajectory_legacy",
        quote_aware=False,
    )

    assert (
        "PARTITION BY event_id, market_id, bookie_id, choice_id, target_minute"
        in session.statement
    )
    assert "traj.quote_id IS NOT NULL" not in session.statement
