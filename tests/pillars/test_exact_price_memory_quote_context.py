from __future__ import annotations

from decimal import Decimal

from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)
from modules.pillars.pillar_5.exact_price_memory_engine.exact_price_memory_engine import (
    _extract_current_price_set,
)


def _choice(name: str, odds: str, quote_id: int) -> ChoiceOddsTrajectory:
    return ChoiceOddsTrajectory(
        choice_name=name,
        choice_id=quote_id,
        initial_odds=Decimal(odds),
        quote_id=quote_id,
        odds_values={0: Decimal(odds)},
    )


def _bookie(*, source: str, side=None, quote_base: int, home: str, away: str):
    return BookieOddsTrajectory(
        bookie_id=1,
        bookie_name="SofaScore",
        source=source,
        exchange_side=side,
        exchange_level=0,
        choices={
            "1": _choice("1", home, quote_base),
            "2": _choice("2", away, quote_base + 1),
        },
    )


def test_pillar_5_selects_only_primary_unsided_sofascore_quote():
    bookies = {
        "1:oddspapi:single:0": _bookie(
            source="oddspapi", quote_base=10, home="9.0", away="9.1"
        ),
        "1:sofascore:back:0": _bookie(
            source="sofascore", side="back", quote_base=20, home="8.0", away="8.1"
        ),
        "1:sofascore:single:0": _bookie(
            source="sofascore", quote_base=30, home="1.8", away="2.1"
        ),
    }
    line = MarketLineOddsTrajectory(
        market_id=1,
        market_name="Match Result",
        market_group="1X2",
        market_period="Full Time",
        choice_group=None,
        bookies=bookies,
    )
    context = OddsTrajectoryContext(
        available=True,
        event_id=1,
        target_minutes_expected=[0],
        target_minutes_present=[0],
        missing_target_minutes=[],
        markets={"1X2": {"Full Time": {"Match Result": {"__default__": line}}}},
    )

    selected = _extract_current_price_set(context)

    assert selected["selected_bookie_name"] == "SofaScore"
    assert selected["candidate_line_count"] == 1
    assert selected["current_home_odds"] == Decimal("1.8")
    assert selected["current_away_odds"] == Decimal("2.1")
