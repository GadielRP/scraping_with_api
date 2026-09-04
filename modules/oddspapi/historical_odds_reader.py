"""Single-pass historical odds reader: one tree walk, two reductions, one DTO."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from typing import Sequence

from shared.timezone_utils import convert_utc_to_local

from modules.oddspapi.historical_odds_as_of import (
    HistoricalOddsAsOfQuote,
    OddspapiHistoricalOddsAsOf,
)
from modules.oddspapi.historical_odds_normalizer import (
    OddspapiHistoricalOddsNormalizer,
)
from modules.oddspapi.historical_odds_change_detector import (
    OddspapiHistoricalOddsChangeDetector,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalOddsReadResult:
    """Shared result of one historical payload read."""

    normalized_payload: dict
    as_of_quotes: tuple[HistoricalOddsAsOfQuote, ...] = ()


class OddspapiHistoricalOddsReader:
    """Walk a raw ``/historical-odds`` tree once and reduce it in place."""

    @classmethod
    def read(
        cls,
        historical_response: dict | None,
        *,
        source_sport_id: str | int | None,
        as_of_targets: Sequence[tuple[int, datetime, datetime]] = (),
        minimum_initial_span_minutes: float = 0.0,
        require_active_quotes: bool = True,
        current_cutoff_utc: datetime | None = None,
        enable_significant_changes: bool = False,
        min_change_magnitude_pct: float = 20.0,
        min_history_hours: float = 24.0,
        flash_reversal_minutes: float = 3.0,
        min_price: float = 1.01,
        kickoff_utc: datetime | None = None,
    ) -> HistoricalOddsReadResult:
        payload = historical_response if isinstance(historical_response, dict) else {}
        bookmakers = payload.get("bookmakers")
        if not isinstance(bookmakers, dict):
            bookmakers = {}

        if enable_significant_changes and kickoff_utc is None:
            logger.warning("Significant-change extraction disabled: missing kickoff_utc")
            enable_significant_changes = False
        extract_as_of = bool(as_of_targets) or enable_significant_changes
        as_of_quotes: list[HistoricalOddsAsOfQuote] = []
        normalized_bookmakers: dict[str, dict] = {}

        for bookmaker_slug, bookmaker_data in bookmakers.items():
            if not isinstance(bookmaker_data, dict):
                continue
            markets = bookmaker_data.get("markets")
            if not isinstance(markets, dict):
                continue
            slug = str(bookmaker_slug)
            as_of_slug = slug.strip().lower()
            normalized_markets: dict[str, dict] = {}

            for market_id, market_data in markets.items():
                if not isinstance(market_data, dict):
                    continue
                outcomes = market_data.get("outcomes")
                if not isinstance(outcomes, dict):
                    continue
                source_market_id = str(market_id)
                normalized_outcomes: dict[str, dict] = {}

                for outcome_id, outcome_data in outcomes.items():
                    if not isinstance(outcome_data, dict):
                        continue
                    players = outcome_data.get("players")
                    if not isinstance(players, dict):
                        continue
                    source_outcome_id = str(outcome_id)
                    normalized_players: dict[str, dict] = {}

                    for player_id, history in players.items():
                        ticks = OddspapiHistoricalOddsNormalizer.ordered_priced_ticks(
                            history
                        )
                        if enable_significant_changes:
                            ticks = OddspapiHistoricalOddsChangeDetector.sanitize_ticks(
                                ticks, kickoff_utc=kickoff_utc, min_price=min_price
                            )
                        player_key = str(player_id)
                        normalized = (
                            OddspapiHistoricalOddsNormalizer.from_ordered_ticks(
                                ticks,
                                minimum_initial_span_minutes=(
                                    minimum_initial_span_minutes
                                ),
                                require_active_quotes=require_active_quotes,
                                current_cutoff_utc=current_cutoff_utc,
                            )
                        )
                        if normalized is not None:
                            normalized_players[player_key] = normalized
                        if extract_as_of and as_of_slug:
                            quotes = None
                            if enable_significant_changes:
                                quotes = OddspapiHistoricalOddsChangeDetector.detect_significant_changes(
                                    ticks,
                                    kickoff_utc=kickoff_utc,
                                    bookmaker_slug=as_of_slug,
                                    source_market_id=source_market_id,
                                    source_outcome_id=source_outcome_id,
                                    player_id=player_key,
                                    to_local=convert_utc_to_local,
                                    min_change_magnitude_pct=min_change_magnitude_pct,
                                    min_history_hours=min_history_hours,
                                    flash_reversal_minutes=flash_reversal_minutes,
                                )
                            # The adaptive reducer returns no moment quotes in
                            # two cases: the series is too short for the
                            # configured history window, or it has sufficient
                            # history but no qualifying change.  Both cases
                            # need the same fixed-moment representation.  An
                            # empty tick series remains empty because the
                            # fallback reducer has no value to carry forward.
                            if quotes is None or not quotes:
                                quotes = OddspapiHistoricalOddsAsOf.from_ordered_ticks(
                                    ticks,
                                    targets=as_of_targets,
                                    bookmaker_slug=as_of_slug,
                                    source_market_id=source_market_id,
                                    source_outcome_id=source_outcome_id,
                                    player_id=player_key,
                                    require_active_quotes=require_active_quotes,
                                )
                            as_of_quotes.extend(quotes)

                    if normalized_players:
                        normalized_outcomes[source_outcome_id] = {
                            "players": normalized_players
                        }

                if normalized_outcomes:
                    normalized_markets[source_market_id] = {
                        "marketActive": True,
                        "outcomes": normalized_outcomes,
                    }

            if normalized_markets:
                normalized_bookmakers[slug] = {"markets": normalized_markets}

        return HistoricalOddsReadResult(
            normalized_payload={
                "fixtureId": payload.get("fixtureId"),
                "sportId": source_sport_id,
                "bookmakerOdds": normalized_bookmakers,
            },
            as_of_quotes=tuple(as_of_quotes),
        )
