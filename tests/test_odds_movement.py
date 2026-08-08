"""Unit tests for compute_movement (extracted from MarketRepository._choice_change).

See docs/refactors/db-schema-odds-refactor.md §7 — this is a 1:1 behavior-
preserving extraction, so these tests cover the same cases the old private
method needed to handle, plus regression coverage for MarketRepository's
thin wrapper.
"""

from infrastructure.persistence.repositories.market.odds_movement import compute_movement
from infrastructure.persistence.repositories.market_repository import MarketRepository


def test_explicit_change_takes_precedence():
    assert compute_movement(explicit_change=1, initial_odds=2.0, current_odds=1.5) == 1
    assert compute_movement(explicit_change=-1, initial_odds=1.5, current_odds=2.0) == -1
    assert compute_movement(explicit_change=0, initial_odds=1.5, current_odds=2.0) == 0


def test_invalid_explicit_change_falls_back_to_comparison():
    assert compute_movement(explicit_change=42, initial_odds=1.5, current_odds=2.0) == 1
    assert compute_movement(explicit_change="not-a-number", initial_odds=2.0, current_odds=1.5) == -1


def test_current_greater_than_initial_is_increase():
    assert compute_movement(initial_odds=1.50, current_odds=1.90) == 1


def test_current_less_than_initial_is_decrease():
    assert compute_movement(initial_odds=1.90, current_odds=1.50) == -1


def test_current_equal_initial_is_unchanged():
    assert compute_movement(initial_odds=1.50, current_odds=1.50) == 0


def test_missing_initial_or_current_returns_none_not_unchanged():
    assert compute_movement(initial_odds=None, current_odds=1.50) is None
    assert compute_movement(initial_odds=1.50, current_odds=None) is None
    assert compute_movement() is None


def test_market_repository_wrapper_delegates_to_compute_movement():
    assert (
        MarketRepository._choice_change(
            explicit_change=None,
            initial_odds=1.50,
            current_odds=1.90,
        )
        == 1
    )
    assert (
        MarketRepository._choice_change(
            explicit_change=None,
            initial_odds=None,
            current_odds=None,
        )
        is None
    )
