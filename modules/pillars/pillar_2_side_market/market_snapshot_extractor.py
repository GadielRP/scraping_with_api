"""Extract the complete, single-minute market snapshot required by P2."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

from modules.pillars.odds_trajectory_context import (
    BookieOddsTrajectory,
    ChoiceOddsTrajectory,
    MarketLineOddsTrajectory,
    OddsTrajectoryContext,
)
from modules.pillars.pillar_2_side_market.models import (
    AsianHandicapSnapshot,
    ExchangeSnapshot,
    P2ExtractionResult,
    P2MarketSnapshot,
    QuotePoint,
    QuoteTrace,
    ThreeWayMarketSnapshot,
    TwoWayMarketSnapshot,
)


# Optional explicit target override for P2.  Keep this as ``None`` to retain
# the default behavior: select the most recent available configured minute
# (the numerically smallest minute, e.g. -5 when both 0 and -5 are present).
P2_HARDCODED_TARGET_MINUTE: int | None = 0
P2_TARGET_MINUTES = frozenset({120, 30, 5, 1, 0, -5})
FT_PERIODS = frozenset({"full time", "full time including overtime"})
FIRST_HALF_PERIODS = frozenset({"1st half"})


def _normalize(value: object) -> str:
    return " ".join(str(value or "").replace("-", " ").casefold().split())


def _bookie_matches(
    bookie: BookieOddsTrajectory,
    *,
    bookie_id: int,
    accepted_names: frozenset[str],
) -> bool:
    return bookie.bookie_id == bookie_id or _normalize(bookie.bookie_name) in accepted_names


def _iter_matching_bookies(
    context: OddsTrajectoryContext,
    *,
    periods: frozenset[str],
    market_name: str,
    bookie_id: int,
    accepted_names: frozenset[str],
    exchange_side: str | None = None,
) -> Iterable[tuple[MarketLineOddsTrajectory, BookieOddsTrajectory]]:
    normalized_market_name = _normalize(market_name)
    for market_periods in context.markets.values():
        for period, market_names in market_periods.items():
            if _normalize(period) not in periods:
                continue
            for current_market_name, market_lines in market_names.items():
                if _normalize(current_market_name) != normalized_market_name:
                    continue
                for market_line in market_lines.values():
                    for bookie in market_line.bookies.values():
                        if not _bookie_matches(
                            bookie,
                            bookie_id=bookie_id,
                            accepted_names=accepted_names,
                        ):
                            continue
                        if bookie.exchange_side != exchange_side:
                            continue
                        yield market_line, bookie


def _choice(bookie: BookieOddsTrajectory, choice_name: str) -> ChoiceOddsTrajectory | None:
    normalized_name = _normalize(choice_name)
    matches = [
        choice
        for current_name, choice in bookie.choices.items()
        if _normalize(current_name) == normalized_name
    ]
    return matches[0] if len(matches) == 1 else None


def _quote_point(
    market_line: MarketLineOddsTrajectory,
    bookie: BookieOddsTrajectory,
    choice: ChoiceOddsTrajectory,
    target_minute: int,
    input_name: str,
    *,
    require_exchange_size: bool,
    missing: set[str],
    invalid: set[str],
) -> QuotePoint | None:
    odds_price = choice.odds_values.get(target_minute)
    if odds_price is None:
        missing.add(input_name)
        return None
    if not odds_price.is_finite() or odds_price <= 0:
        invalid.add(input_name)
        return None

    meta = choice.meta_by_minute.get(target_minute)
    exchange_size = meta.exchange_size if meta is not None else None
    if require_exchange_size:
        size_name = input_name.replace("ODDS_PRICE", "EXCHANGE_SIZE")
        if exchange_size is None:
            missing.add(size_name)
            return None
        if not exchange_size.is_finite() or exchange_size < 0:
            invalid.add(size_name)
            return None

    return QuotePoint(
        odds_price=odds_price,
        exchange_size=exchange_size,
        trace=QuoteTrace(
            target_minute=target_minute,
            snapshot_id=meta.snapshot_id if meta is not None else None,
            collected_at=meta.collected_at if meta is not None else None,
            minutes_before_start=meta.minutes_before_start if meta is not None else None,
            market_group=market_line.market_group,
            market_period=market_line.market_period,
            market_name=market_line.market_name,
            choice_group=market_line.choice_group,
            bookie_id=bookie.bookie_id,
            bookie_name=bookie.bookie_name,
            source=bookie.source,
            exchange_side=bookie.exchange_side,
            exchange_level=bookie.exchange_level,
            choice_name=choice.choice_name,
        ),
    )


def _extract_two_way(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    periods: frozenset[str],
    market_name: str,
    bookie_id: int,
    accepted_names: frozenset[str],
    home_input: str,
    away_input: str,
    missing: set[str],
    invalid: set[str],
    ambiguous: set[str],
) -> TwoWayMarketSnapshot | None:
    candidates: list[TwoWayMarketSnapshot] = []
    saw_matching_bookie = False
    for market_line, bookie in _iter_matching_bookies(
        context,
        periods=periods,
        market_name=market_name,
        bookie_id=bookie_id,
        accepted_names=accepted_names,
    ):
        saw_matching_bookie = True
        home_choice = _choice(bookie, "1")
        away_choice = _choice(bookie, "2")
        if home_choice is None:
            missing.add(home_input)
        if away_choice is None:
            missing.add(away_input)
        if home_choice is None or away_choice is None:
            continue
        local_missing: set[str] = set()
        local_invalid: set[str] = set()
        home = _quote_point(
            market_line,
            bookie,
            home_choice,
            target_minute,
            home_input,
            require_exchange_size=False,
            missing=local_missing,
            invalid=local_invalid,
        )
        away = _quote_point(
            market_line,
            bookie,
            away_choice,
            target_minute,
            away_input,
            require_exchange_size=False,
            missing=local_missing,
            invalid=local_invalid,
        )
        missing.update(local_missing)
        invalid.update(local_invalid)
        if home is not None and away is not None:
            candidates.append(TwoWayMarketSnapshot(home=home, away=away))

    if not saw_matching_bookie:
        missing.update({home_input, away_input})
    if len(candidates) > 1:
        ambiguous.update({home_input, away_input})
        return None
    return candidates[0] if candidates else None


def _extract_ah(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    periods: frozenset[str],
    market_name: str,
    bookie_id: int,
    accepted_names: frozenset[str],
    line_input: str,
    home_input: str,
    away_input: str,
    missing: set[str],
    invalid: set[str],
    ambiguous: set[str],
) -> AsianHandicapSnapshot | None:
    candidates: list[AsianHandicapSnapshot] = []
    saw_matching_bookie = False
    for market_line, bookie in _iter_matching_bookies(
        context,
        periods=periods,
        market_name=market_name,
        bookie_id=bookie_id,
        accepted_names=accepted_names,
    ):
        saw_matching_bookie = True
        try:
            line = Decimal(str(market_line.choice_group))
        except (InvalidOperation, TypeError, ValueError):
            invalid.add(line_input)
            continue
        if not line.is_finite():
            invalid.add(line_input)
            continue

        home_choice = _choice(bookie, "1")
        away_choice = _choice(bookie, "2")
        if home_choice is None:
            missing.add(home_input)
        if away_choice is None:
            missing.add(away_input)
        if home_choice is None or away_choice is None:
            continue
        local_missing: set[str] = set()
        local_invalid: set[str] = set()
        home = _quote_point(
            market_line,
            bookie,
            home_choice,
            target_minute,
            home_input,
            require_exchange_size=False,
            missing=local_missing,
            invalid=local_invalid,
        )
        away = _quote_point(
            market_line,
            bookie,
            away_choice,
            target_minute,
            away_input,
            require_exchange_size=False,
            missing=local_missing,
            invalid=local_invalid,
        )
        missing.update(local_missing)
        invalid.update(local_invalid)
        if home is not None and away is not None:
            candidates.append(AsianHandicapSnapshot(home=home, away=away, home_line=line))

    if not saw_matching_bookie:
        missing.update({line_input, home_input, away_input})
    if len(candidates) > 1:
        ambiguous.update({line_input, home_input, away_input})
        return None
    return candidates[0] if candidates else None


def _extract_exchange_side(
    context: OddsTrajectoryContext,
    *,
    target_minute: int,
    exchange_side: str,
    missing: set[str],
    invalid: set[str],
    ambiguous: set[str],
) -> ThreeWayMarketSnapshot | None:
    side_label = exchange_side.upper()
    names = {
        "1": f"BF_HOME_{side_label}_FULL_TIME_ODDS_PRICE",
        "x": f"BF_DRAW_{side_label}_FULL_TIME_ODDS_PRICE",
        "2": f"BF_AWAY_{side_label}_FULL_TIME_ODDS_PRICE",
    }
    candidates: list[ThreeWayMarketSnapshot] = []
    saw_matching_bookie = False
    for market_line, bookie in _iter_matching_bookies(
        context,
        periods=FT_PERIODS,
        market_name="1X2 Full Time",
        bookie_id=4,
        accepted_names=frozenset({"betfair", "betfair exchange"}),
        exchange_side=exchange_side,
    ):
        saw_matching_bookie = True
        choices = {name: _choice(bookie, name) for name in names}
        for choice_name, choice in choices.items():
            if choice is None:
                missing.add(names[choice_name])
        if any(choice is None for choice in choices.values()):
            continue

        local_missing: set[str] = set()
        local_invalid: set[str] = set()
        points = {
            choice_name: _quote_point(
                market_line,
                bookie,
                choice,  # type: ignore[arg-type]
                target_minute,
                names[choice_name],
                require_exchange_size=True,
                missing=local_missing,
                invalid=local_invalid,
            )
            for choice_name, choice in choices.items()
        }
        missing.update(local_missing)
        invalid.update(local_invalid)
        if all(point is not None for point in points.values()):
            candidates.append(
                ThreeWayMarketSnapshot(
                    home=points["1"],  # type: ignore[arg-type]
                    draw=points["x"],  # type: ignore[arg-type]
                    away=points["2"],  # type: ignore[arg-type]
                )
            )

    if not saw_matching_bookie:
        missing.update(names.values())
        missing.update(name.replace("ODDS_PRICE", "EXCHANGE_SIZE") for name in names.values())
    if len(candidates) > 1:
        ambiguous.update(names.values())
        return None
    return candidates[0] if candidates else None


def extract_p2_market_snapshot(context: OddsTrajectoryContext) -> P2ExtractionResult:
    """Apply the universal minute and completeness gates from the P2 blueprint."""
    if P2_HARDCODED_TARGET_MINUTE is not None:
        # A configured override is intentionally strict: do not silently fall
        # back to another minute when the requested target is unavailable.
        target_minute = int(P2_HARDCODED_TARGET_MINUTE)
    else:
        configured_minutes = [
            minute
            for minute in context.target_minutes_present
            if minute in P2_TARGET_MINUTES
        ]
        if not configured_minutes:
            return P2ExtractionResult(snapshot=None, target_minute=None)
        target_minute = min(configured_minutes)

    missing: set[str] = set()
    invalid: set[str] = set()
    ambiguous: set[str] = set()
    pinnacle_names = frozenset({"pinnacle", "pinnacle sports"})
    bet365_names = frozenset({"bet365"})

    common = {
        "context": context,
        "target_minute": target_minute,
        "missing": missing,
        "invalid": invalid,
        "ambiguous": ambiguous,
    }
    pin_ft_1x2 = _extract_two_way(
        **common,
        periods=FT_PERIODS,
        market_name="1X2 Full Time",
        bookie_id=302,
        accepted_names=pinnacle_names,
        home_input="PIN_HOME_1X2_FULL_TIME_ODDS_PRICE",
        away_input="PIN_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    )
    b365_ft_1x2 = _extract_two_way(
        **common,
        periods=FT_PERIODS,
        market_name="1X2 Full Time",
        bookie_id=3,
        accepted_names=bet365_names,
        home_input="B365_HOME_1X2_FULL_TIME_ODDS_PRICE",
        away_input="B365_AWAY_1X2_FULL_TIME_ODDS_PRICE",
    )
    pin_ft_ah = _extract_ah(
        **common,
        periods=FT_PERIODS,
        market_name="Asian Handicap Full Time",
        bookie_id=302,
        accepted_names=pinnacle_names,
        line_input="PIN_AH_FULL_TIME_LINE",
        home_input="PIN_AH_HOME_FULL_TIME_ODDS_PRICE",
        away_input="PIN_AH_AWAY_FULL_TIME_ODDS_PRICE",
    )
    b365_ft_ah = _extract_ah(
        **common,
        periods=FT_PERIODS,
        market_name="Asian Handicap Full Time",
        bookie_id=3,
        accepted_names=bet365_names,
        line_input="B365_AH_FULL_TIME_LINE",
        home_input="B365_AH_HOME_FULL_TIME_ODDS_PRICE",
        away_input="B365_AH_AWAY_FULL_TIME_ODDS_PRICE",
    )
    bf_back = _extract_exchange_side(
        context,
        target_minute=target_minute,
        exchange_side="back",
        missing=missing,
        invalid=invalid,
        ambiguous=ambiguous,
    )
    bf_lay = _extract_exchange_side(
        context,
        target_minute=target_minute,
        exchange_side="lay",
        missing=missing,
        invalid=invalid,
        ambiguous=ambiguous,
    )
    pin_1h_1x2 = _extract_two_way(
        **common,
        periods=FIRST_HALF_PERIODS,
        market_name="1X2 First Half",
        bookie_id=302,
        accepted_names=pinnacle_names,
        home_input="PIN_HOME_1X2_1H_ODDS_PRICE",
        away_input="PIN_AWAY_1X2_1H_ODDS_PRICE",
    )
    b365_1h_1x2 = _extract_two_way(
        **common,
        periods=FIRST_HALF_PERIODS,
        market_name="1X2 First Half",
        bookie_id=3,
        accepted_names=bet365_names,
        home_input="B365_HOME_1X2_1H_ODDS_PRICE",
        away_input="B365_AWAY_1X2_1H_ODDS_PRICE",
    )
    pin_1h_ah = _extract_ah(
        **common,
        periods=FIRST_HALF_PERIODS,
        market_name="Asian Handicap First Half",
        bookie_id=302,
        accepted_names=pinnacle_names,
        line_input="PIN_AH_1H_LINE",
        home_input="PIN_AH_1H_HOME_PRICE",
        away_input="PIN_AH_1H_AWAY_PRICE",
    )
    b365_1h_ah = _extract_ah(
        **common,
        periods=FIRST_HALF_PERIODS,
        market_name="Asian Handicap First Half",
        bookie_id=3,
        accepted_names=bet365_names,
        line_input="B365_AH_1H_LINE",
        home_input="B365_AH_1H_HOME_PRICE",
        away_input="B365_AH_1H_AWAY_PRICE",
    )

    components = (
        pin_ft_1x2,
        b365_ft_1x2,
        pin_ft_ah,
        b365_ft_ah,
        bf_back,
        bf_lay,
        pin_1h_1x2,
        b365_1h_1x2,
        pin_1h_ah,
        b365_1h_ah,
    )
    if missing or invalid or ambiguous or any(component is None for component in components):
        return P2ExtractionResult(
            snapshot=None,
            target_minute=target_minute,
            missing_inputs=tuple(sorted(missing)),
            invalid_inputs=tuple(sorted(invalid)),
            ambiguous_inputs=tuple(sorted(ambiguous)),
        )

    return P2ExtractionResult(
        snapshot=P2MarketSnapshot(
            target_minute=target_minute,
            pinnacle_ft_1x2=pin_ft_1x2,  # type: ignore[arg-type]
            bet365_ft_1x2=b365_ft_1x2,  # type: ignore[arg-type]
            pinnacle_ft_ah=pin_ft_ah,  # type: ignore[arg-type]
            bet365_ft_ah=b365_ft_ah,  # type: ignore[arg-type]
            betfair_ft_1x2=ExchangeSnapshot(
                back=bf_back,  # type: ignore[arg-type]
                lay=bf_lay,  # type: ignore[arg-type]
            ),
            pinnacle_1h_1x2=pin_1h_1x2,  # type: ignore[arg-type]
            bet365_1h_1x2=b365_1h_1x2,  # type: ignore[arg-type]
            pinnacle_1h_ah=pin_1h_ah,  # type: ignore[arg-type]
            bet365_1h_ah=b365_1h_ah,  # type: ignore[arg-type]
        ),
        target_minute=target_minute,
    )
