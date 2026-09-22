from __future__ import annotations

from modules.pillars.market_snapshot_extractor import TargetMinuteSelection
from modules.pillars.odds_trajectory_context import (
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)
from modules.pillars.pillar_5.market_selection import select_p5_moneyline_target
from modules.pillars.pillar_5.snapshot_policy import extract_p5_market_snapshot


def _context(*identities: tuple[str, str, str]) -> OddsTrajectoryContext:
    markets: dict = {}
    for index, (group, period, name) in enumerate(identities, start=1):
        markets.setdefault(group, {}).setdefault(period, {}).setdefault(name, {})[
            "__default__"
        ] = MarketLineOddsTrajectory(
            market_id=index,
            market_name=name,
            market_group=group,
            market_period=period,
            line_value=None,
            bookies={},
        )
    return OddsTrajectoryContext(
        available=True,
        event_id=277981,
        target_minutes_expected=[5],
        target_minutes_present=[5],
        missing_target_minutes=[],
        markets=markets,
    )


def test_selects_the_only_canonical_moneyline_contract() -> None:
    result = select_p5_moneyline_target(
        _context(
            (
                "Home/Away",
                "Full Time Including Overtime",
                "Home/Away Full Time Including Overtime",
            ),
        )
    )

    assert result.reason is None
    assert result.target is not None
    assert result.target.market_group == "Home/Away"
    assert result.target.market_period == "Full Time Including Overtime"
    assert result.target.is_two_way is True


def test_rejects_multiple_moneyline_contracts_as_ambiguous() -> None:
    result = select_p5_moneyline_target(
        _context(
            ("1X2", "Full Time", "1X2 Full Time"),
            (
                "Home/Away",
                "Full Time Including Overtime",
                "Home/Away Full Time Including Overtime",
            ),
        )
    )

    assert result.target is None
    assert result.reason == "ambiguous_moneyline_market"
    assert len(result.candidates) == 2
    assert result.is_ambiguous is True


def test_does_not_treat_period_aliases_as_the_same_contract() -> None:
    result = select_p5_moneyline_target(
        _context(
            (
                "Home/Away",
                "Full Time Including Overtime",
                "Home/Away Full Time",
            ),
        )
    )

    assert result.target is None
    assert result.reason == "moneyline_market_unavailable"


def test_snapshot_extraction_fails_closed_on_moneyline_ambiguity() -> None:
    result = extract_p5_market_snapshot(
        277981,
        _context(
            ("1X2", "Full Time", "1X2 Full Time"),
            (
                "Home/Away",
                "Full Time Including Overtime",
                "Home/Away Full Time Including Overtime",
            ),
        ),
        TargetMinuteSelection(target_minute=5),
    )

    assert result.full_time_snapshot is None
    assert result.abort_reason == "ambiguous_moneyline_market"
    assert result.full_time.status == "AMBIGUOUS"
    assert result.ambiguous_inputs == ("P5_MONEYLINE_TARGET",)
    selection = result.extraction_diagnostics["moneyline_selection"]
    assert selection["target"] is None
    assert len(selection["candidates"]) == 2
