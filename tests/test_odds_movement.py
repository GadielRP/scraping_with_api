"""Unit tests for the canonical odds-movement policy."""

from infrastructure.persistence.repositories.market.odds_movement import compute_movement


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
