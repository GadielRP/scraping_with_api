"""Compare reconstructed historical-as-of quotes against already stored snapshots.

This observer does not read `/historical-odds`. It consumes `as_of_quotes`
already produced by `OddspapiHistoricalOddsReader` during live acquisition.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Sequence

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from modules.oddspapi.historical_odds_as_of import HistoricalOddsAsOfQuote

logger = logging.getLogger(__name__)


def log_historical_odds_as_of_shadow(
    *,
    event_id: int,
    fixture_id: str | None,
    as_of_quotes: Sequence[HistoricalOddsAsOfQuote],
    tolerance_minutes: int = 3,
) -> None:
    if not as_of_quotes:
        logger.info(
            "Oddspapi historical as-of shadow event_id=%s fixture_id=%s reconstructed=0",
            event_id,
            fixture_id,
        )
        return

    existing = _load_existing_prices(event_id)
    compared = 0
    matched = 0
    missing = 0
    mismatched = 0
    for quote in as_of_quotes:
        stored = _closest_price(
            existing,
            source_outcome_id=quote.source_outcome_id,
            collected_at=quote.collected_at,
            tolerance_minutes=tolerance_minutes,
        )
        compared += 1
        if stored is None:
            missing += 1
            continue
        if abs(float(stored) - float(quote.price)) <= 0.001:
            matched += 1
        else:
            mismatched += 1
            logger.info(
                "Oddspapi historical as-of mismatch event_id=%s outcome=%s "
                "moment=%s reconstructed=%s stored=%s",
                event_id,
                quote.source_outcome_id,
                quote.minutes_until_start,
                quote.price,
                stored,
            )
    logger.info(
        "Oddspapi historical as-of shadow event_id=%s fixture_id=%s "
        "reconstructed=%s compared=%s matched=%s missing=%s mismatched=%s",
        event_id,
        fixture_id,
        len(as_of_quotes),
        compared,
        matched,
        missing,
        mismatched,
    )


def _load_existing_prices(event_id: int) -> list[tuple[str, datetime, float]]:
    try:
        with db_manager.get_session() as session:
            rows = (
                session.query(
                    MarketChoiceQuote.source_outcome_id,
                    MarketChoiceSnapshot.collected_at,
                    MarketChoiceSnapshot.odds_value,
                )
                .join(
                    MarketChoiceQuote,
                    MarketChoiceSnapshot.quote_id == MarketChoiceQuote.quote_id,
                )
                .join(
                    MarketChoice,
                    MarketChoiceQuote.choice_id == MarketChoice.choice_id,
                )
                .join(Market, MarketChoice.market_id == Market.market_id)
                .filter(
                    Market.event_id == int(event_id),
                    MarketChoiceQuote.source == "oddspapi",
                )
                .all()
            )
    except Exception as exc:
        logger.warning(
            "Oddspapi historical as-of shadow lookup failed event_id=%s: %s",
            event_id,
            exc,
        )
        return []
    loaded: list[tuple[str, datetime, float]] = []
    for outcome_id, collected_at, odds_value in rows:
        if outcome_id is None or collected_at is None or odds_value is None:
            continue
        loaded.append((str(outcome_id).strip(), collected_at, float(odds_value)))
    return loaded


def _closest_price(
    existing: list[tuple[str, datetime, float]],
    *,
    source_outcome_id: str,
    collected_at: datetime,
    tolerance_minutes: int,
) -> float | None:
    window = timedelta(minutes=max(0, int(tolerance_minutes)))
    best: tuple[timedelta, float] | None = None
    for outcome_id, stored_at, price in existing:
        if outcome_id != source_outcome_id:
            continue
        distance = abs(stored_at - collected_at)
        if distance > window:
            continue
        if best is None or distance < best[0]:
            best = (distance, price)
    return None if best is None else best[1]
