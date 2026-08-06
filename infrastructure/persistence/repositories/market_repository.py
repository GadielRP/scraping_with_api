import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, selectinload

from infrastructure.persistence.models import Market, MarketChoice, MarketChoiceSnapshot
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.market_write_policy import (
    market_write_policy_for_source,
)
from infrastructure.persistence.repositories.bookie_repository import BookieRepository
from shared.odds_utils import fractional_to_decimal, normalize_odds_value
from shared.timezone_utils import convert_utc_to_local, get_local_now

logger = logging.getLogger(__name__)
oddsportal_logger = logging.LoggerAdapter(logger, {"oddsportal": True})


@dataclass
class MarketSaveResult:
    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0


class MarketRepository:
    """
    Repository for storing and retrieving dynamic odds markets.

    Each event can have multiple markets (Full time, Match goals 2.5, Asian handicap, etc.)
    Each market has multiple choices stored in MarketChoice table.
    """

    @staticmethod
    def _fractional_to_decimal(fractional: str) -> float:
        """
        Convert fractional odds to decimal.

        Examples:
            "53/100" -> 1.53
            "27/10" -> 3.7
            "17/4" -> 5.25
        """
        decimal_value = fractional_to_decimal(fractional)
        return float(decimal_value) if decimal_value is not None else None

    @staticmethod
    def _normalize_string_or_none(val: str) -> Optional[str]:
        if val is None:
            return None
        val_stripped = str(val).strip()
        return val_stripped if val_stripped else None

    @staticmethod
    def _parse_source_datetime(
        value,
        *,
        convert_to_project_timezone: bool = False,
    ) -> Optional[datetime]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            parsed = value
        else:
            normalized = str(value).strip()
            if not normalized:
                return None
            if normalized.endswith("Z"):
                normalized = normalized[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None
        if convert_to_project_timezone:
            return convert_utc_to_local(parsed)
        return parsed

    @staticmethod
    def _uses_utc_source_timestamps(source: str | None) -> bool:
        return str(source or "").strip().lower().startswith("oddspapi")

    @staticmethod
    def _numeric_or_none(value):
        if value in (None, ""):
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _float_or_none(value):
        normalized = normalize_odds_value(value)
        return float(normalized) if normalized is not None else None

    @staticmethod
    def _choice_change(
        *,
        explicit_change,
        initial_odds,
        current_odds,
    ) -> Optional[int]:
        if explicit_change not in (None, ""):
            normalized_change = MarketRepository._numeric_or_none(
                explicit_change
            )
            if normalized_change in {
                Decimal("-1"),
                Decimal("0"),
                Decimal("1"),
            }:
                return int(normalized_change)

        initial = MarketRepository._numeric_or_none(initial_odds)
        current = MarketRepository._numeric_or_none(current_odds)
        if initial is None or current is None:
            return None
        if current > initial:
            return 1
        if current < initial:
            return -1
        return 0

    @staticmethod
    def _slugify_source_bookie_name(name: str) -> str:
        normalized = str(name or "").strip().lower()
        if not normalized:
            return ""
        normalized = normalized.replace("&", " and ")
        normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
        return normalized

    @staticmethod
    def _build_single_market_response(
        market_name: str,
        market_group: Optional[str],
        market_period: Optional[str],
        choice_group: Optional[str],
        choices: List[Dict],
        is_live: bool = False,
    ) -> Dict:
        return {
            "markets": [
                {
                    "marketName": market_name,
                    "marketGroup": market_group,
                    "marketPeriod": market_period,
                    "choiceGroup": choice_group,
                    "isLive": is_live,
                    "choices": choices,
                }
            ]
        }

    @staticmethod
    def save_markets_from_response(event_id: int, odds_response: Dict, bookie_id: int) -> int:
        """Save all markets from an odds API response to the database."""
        return MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=odds_response,
            bookie_id=bookie_id,
        ).markets_saved

    @staticmethod
    def save_markets_from_response_with_stats(
        event_id: int,
        odds_response: Dict,
        bookie_id: int,
        source: Optional[str] = None,
    ) -> MarketSaveResult:
        write_policy = market_write_policy_for_source(source)
        operation_logger = (
            oddsportal_logger if source == "oddsportal" else logger
        )
        try:
            if bookie_id is None:
                operation_logger.error(
                    "Cannot save markets for event %s without an explicit bookie_id",
                    event_id,
                )
                return MarketSaveResult()

            markets_data = odds_response.get('markets', [])
            if not markets_data:
                operation_logger.debug(
                    f"No markets in odds response for event {event_id}"
                )
                return MarketSaveResult()

            result = MarketSaveResult()

            with db_manager.get_session() as session:
                for market_data in markets_data:
                    try:
                        with session.begin_nested():
                            market_name = MarketRepository._normalize_market_name(market_data.get('marketName'))
                            market_group = MarketRepository._normalize_market_group(market_data.get('marketGroup'))
                            market_period_normalized = MarketRepository._normalize_market_period(market_data.get('marketPeriod'))
                            choice_group_normalized = MarketRepository._normalize_string_or_none(market_data.get('choiceGroup'))
                            is_live = market_data.get('isLive', False)

                            if not market_name:
                                operation_logger.info(
                                    "Skipping market for event %s because marketName is missing",
                                    event_id,
                                )
                                continue

                            choices_data = market_data.get('choices', [])
                            seen_choice_names = {}
                            for choice_data in choices_data:
                                choice_name = MarketRepository._normalize_string_or_none(
                                    choice_data.get('name')
                                )
                                if not choice_name or choice_name in seen_choice_names:
                                    continue
                                if (
                                    write_policy.require_initial_odds
                                    and MarketRepository._choice_odds_value(
                                        choice_data,
                                        "initialFractionalValue",
                                        "initialDecimalValue",
                                        "initialOdds",
                                        "initial_odds",
                                    )
                                    is None
                                ):
                                    continue
                                seen_choice_names[choice_name] = choice_data
                            if not seen_choice_names:
                                operation_logger.info(
                                    "Skipping market for event %s because policy=%s "
                                    "found no eligible choices",
                                    event_id,
                                    write_policy.name,
                                )
                                continue

                            market_collected_at = get_local_now()
                            existing_market = session.query(Market).filter(
                                and_(
                                    Market.event_id == event_id,
                                    Market.bookie_id == bookie_id,
                                    Market.market_name == market_name,
                                    Market.market_period == market_period_normalized,
                                    or_(Market.choice_group == choice_group_normalized, Market.choice_group == "") if choice_group_normalized is None else Market.choice_group == choice_group_normalized,
                                    Market.is_live == is_live
                                )
                            ).first()

                            if existing_market:
                                market = existing_market
                                market.market_group = market_group
                                market.market_period = market_period_normalized
                                market.collected_at = market_collected_at
                            else:
                                market = Market(
                                    event_id=event_id,
                                    bookie_id=bookie_id,
                                    market_name=market_name,
                                    market_group=market_group,
                                    market_period=market_period_normalized,
                                    choice_group=choice_group_normalized,
                                    is_live=is_live,
                                    collected_at=market_collected_at
                                )
                                session.add(market)
                                session.flush()

                            uses_oddspapi_source_time = (
                                MarketRepository._uses_utc_source_timestamps(
                                    source
                                )
                            )
                            for choice_name, choice_data in seen_choice_names.items():
                                initial_odds = MarketRepository._choice_odds_value(
                                    choice_data,
                                    "initialFractionalValue",
                                    "initialDecimalValue",
                                    "initialOdds",
                                    "initial_odds",
                                )
                                current_odds = MarketRepository._choice_odds_value(
                                    choice_data,
                                    "fractionalValue",
                                    "decimalValue",
                                    "currentOdds",
                                    "current_odds",
                                    "odds",
                                )

                                existing_choice = session.query(MarketChoice).filter(
                                    and_(
                                        MarketChoice.market_id == market.market_id,
                                        MarketChoice.choice_name == choice_name
                                    )
                                ).first()

                                if (
                                    write_policy.overwrite_initial_odds
                                    and initial_odds is not None
                                ):
                                    effective_initial_odds = initial_odds
                                elif (
                                    existing_choice
                                    and existing_choice.initial_odds is not None
                                ):
                                    effective_initial_odds = existing_choice.initial_odds
                                else:
                                    effective_initial_odds = initial_odds
                                effective_current_odds = (
                                    current_odds
                                    if write_policy.persist_current_odds
                                    else (
                                        existing_choice.current_odds
                                        if existing_choice is not None
                                        else None
                                    )
                                )
                                change = MarketRepository._choice_change(
                                    explicit_change=(
                                        choice_data.get('change')
                                        if write_policy.persist_current_odds
                                        else None
                                    ),
                                    initial_odds=effective_initial_odds,
                                    current_odds=effective_current_odds,
                                )

                                initial_was_set = False
                                if existing_choice:
                                    if (
                                        write_policy.persist_current_odds
                                        and current_odds is not None
                                    ):
                                        existing_choice.current_odds = current_odds
                                    if change is not None:
                                        existing_choice.change = change
                                    if (
                                        write_policy.overwrite_initial_odds
                                        and initial_odds is not None
                                    ):
                                        existing_initial = MarketRepository._numeric_or_none(
                                            existing_choice.initial_odds
                                        )
                                        incoming_initial = MarketRepository._numeric_or_none(
                                            initial_odds
                                        )
                                        if existing_initial != incoming_initial:
                                            existing_choice.initial_odds = initial_odds
                                            initial_was_set = True
                                    elif existing_choice.initial_odds is None and initial_odds is not None:
                                        existing_choice.initial_odds = initial_odds
                                        initial_was_set = True
                                    choice = existing_choice
                                else:
                                    choice = MarketChoice(
                                        market_id=market.market_id,
                                        choice_name=choice_name,
                                        initial_odds=initial_odds,
                                        current_odds=effective_current_odds,
                                        change=change if change is not None else 0
                                    )
                                    session.add(choice)
                                    session.flush()
                                    initial_was_set = initial_odds is not None

                                source_collected_at = MarketRepository._parse_source_datetime(
                                    choice_data.get("changedAt") or choice_data.get("sourceCollectedAt"),
                                    convert_to_project_timezone=uses_oddspapi_source_time,
                                )
                                snapshot_fields = {
                                    "choice_id": choice.choice_id,
                                    "collected_at": market.collected_at,
                                    "source": source,
                                    "source_collected_at": source_collected_at,
                                    "source_market_id": choice_data.get('sourceMarketId'),
                                    "source_outcome_id": choice_data.get('sourceOutcomeId'),
                                    "bookmaker_outcome_id": choice_data.get('bookmakerOutcomeId'),
                                    "main_line": choice_data.get('mainLine'),
                                    "source_limit": MarketRepository._numeric_or_none(choice_data.get('limit')),
                                }

                                exchange_quotes = choice_data.get("exchangeQuotes")
                                initial_source_collected_at = (
                                    MarketRepository._parse_source_datetime(
                                        choice_data.get("initialChangedAt"),
                                        convert_to_project_timezone=uses_oddspapi_source_time,
                                    )
                                )
                                if (
                                    write_policy.persist_opening_snapshots
                                    and initial_was_set
                                    and initial_odds is not None
                                    and initial_source_collected_at is not None
                                ):
                                    is_exchange_opening = isinstance(
                                        exchange_quotes,
                                        list,
                                    )
                                    session.add(
                                        MarketChoiceSnapshot(
                                            choice_id=choice.choice_id,
                                            odds_value=initial_odds,
                                            collected_at=market.collected_at,
                                            source=source,
                                            source_collected_at=(
                                                initial_source_collected_at
                                            ),
                                            source_market_id=choice_data.get(
                                                'sourceMarketId'
                                            ),
                                            source_outcome_id=choice_data.get(
                                                'sourceOutcomeId'
                                            ),
                                            bookmaker_outcome_id=choice_data.get(
                                                'bookmakerOutcomeId'
                                            ),
                                            main_line=choice_data.get('mainLine'),
                                            source_limit=(
                                                MarketRepository._numeric_or_none(
                                                    choice_data.get(
                                                        "initialLimit"
                                                    )
                                                )
                                            ),
                                            exchange_side=(
                                                "back"
                                                if is_exchange_opening
                                                else None
                                            ),
                                            exchange_level=(
                                                0
                                                if is_exchange_opening
                                                else None
                                            ),
                                            exchange_size=(
                                                MarketRepository._numeric_or_none(
                                                    choice_data.get(
                                                        "initialLimit"
                                                    )
                                                )
                                                if is_exchange_opening
                                                else None
                                            ),
                                        )
                                    )
                                    result.snapshots_saved += 1

                                if (
                                    write_policy.persist_current_snapshots
                                    and isinstance(exchange_quotes, list)
                                ):
                                    for quote in exchange_quotes:
                                        if not isinstance(quote, dict):
                                            continue
                                        quote_price = MarketRepository._float_or_none(quote.get("price"))
                                        quote_side = str(quote.get("side") or "").strip().lower()
                                        try:
                                            quote_level = int(quote.get("level"))
                                        except (TypeError, ValueError):
                                            continue
                                        if quote_price is None or quote_side not in {"back", "lay"}:
                                            continue

                                        session.add(
                                            MarketChoiceSnapshot(
                                                odds_value=quote_price,
                                                exchange_side=quote_side,
                                                exchange_level=quote_level,
                                                exchange_size=MarketRepository._numeric_or_none(
                                                    quote.get("size")
                                                ),
                                                **snapshot_fields,
                                            )
                                        )
                                        result.snapshots_saved += 1
                                elif (
                                    write_policy.persist_current_snapshots
                                    and effective_current_odds is not None
                                ):
                                    session.add(
                                        MarketChoiceSnapshot(
                                            odds_value=effective_current_odds,
                                            exchange_side=None,
                                            exchange_level=None,
                                            exchange_size=None,
                                            **snapshot_fields,
                                        )
                                    )
                                    result.snapshots_saved += 1

                                result.choices_saved += 1

                            result.markets_saved += 1
                    except Exception as e:
                        operation_logger.warning(
                            f"Error processing market for event {event_id}: {e}"
                        )
                        continue

                session.commit()
                if source:
                    operation_logger.info(
                        "Saved %s markets, %s choices and %s snapshots for event %s "
                        "(source=%s policy=%s)",
                        result.markets_saved,
                        result.choices_saved,
                        result.snapshots_saved,
                        event_id,
                        source,
                        write_policy.name,
                    )
                else:
                    operation_logger.info(
                        "Saved %s markets, %s choices and %s snapshots for event %s",
                        result.markets_saved,
                        result.choices_saved,
                        result.snapshots_saved,
                        event_id,
                    )
                return result

        except Exception as e:
            operation_logger.error(
                f"Error saving markets for event {event_id}: {e}"
            )
            return MarketSaveResult()

    @staticmethod
    def save_canonical_bookmaker_batches(
        event_id: int,
        bookmaker_batches: List[Dict],
        *,
        source: str,
    ) -> MarketSaveResult:
        """Persist all canonical bookmaker markets for one event atomically.

        Reference resolution happens before this boundary. This method owns one
        short session/transaction and preloads existing markets plus choices,
        avoiding a session and SELECT pair for every scraped market.
        """
        write_policy = market_write_policy_for_source(source)
        batches = [batch for batch in bookmaker_batches or [] if batch.get("markets")]
        if not batches:
            return MarketSaveResult()

        bookie_ids = {
            int(batch["bookie_id"])
            for batch in batches
            if batch.get("bookie_id") is not None
        }
        if not bookie_ids:
            return MarketSaveResult()

        result = MarketSaveResult()
        collected_at = get_local_now()
        with db_manager.get_session() as session:
            existing_markets = (
                session.query(Market)
                .options(selectinload(Market.choices))
                .filter(
                    Market.event_id == event_id,
                    Market.bookie_id.in_(bookie_ids),
                )
                .all()
            )

            def market_identity(bookie_id, name, period, choice_group, is_live):
                return (
                    int(bookie_id),
                    MarketRepository._normalize_market_name(name),
                    MarketRepository._normalize_market_period(period),
                    MarketRepository._normalize_string_or_none(choice_group),
                    bool(is_live),
                )

            market_index = {
                market_identity(
                    market.bookie_id,
                    market.market_name,
                    market.market_period,
                    market.choice_group,
                    market.is_live,
                ): market
                for market in existing_markets
            }
            legacy_full_time_index = {}
            for market in existing_markets:
                legacy_name = str(market.market_name or "").strip().casefold()
                legacy_period = str(market.market_period or "").strip().casefold()
                if legacy_name not in {"full time", "full-time"}:
                    continue
                if legacy_period not in {"full time", "full-time"}:
                    continue
                legacy_key = (
                    int(market.bookie_id),
                    MarketRepository._normalize_market_group(market.market_group),
                    MarketRepository._normalize_string_or_none(market.choice_group),
                    bool(market.is_live),
                )
                # Ambiguous legacy rows are never guessed. A unique candidate
                # can be upgraded in place on its first canonical write.
                if legacy_key in legacy_full_time_index:
                    legacy_full_time_index[legacy_key] = None
                else:
                    legacy_full_time_index[legacy_key] = market
            prepared_markets = []

            for batch in batches:
                bookie_id = int(batch["bookie_id"])
                for market_data in batch.get("markets") or []:
                    eligible_choices = []
                    seen_choice_names = set()
                    for choice_data in market_data.get("choices") or []:
                        choice_name = MarketRepository._normalize_string_or_none(
                            choice_data.get("name")
                        )
                        if not choice_name or choice_name in seen_choice_names:
                            continue
                        seen_choice_names.add(choice_name)
                        if (
                            write_policy.require_initial_odds
                            and MarketRepository._choice_odds_value(
                                choice_data,
                                "initialFractionalValue",
                                "initialDecimalValue",
                                "initialOdds",
                                "initial_odds",
                            )
                            is None
                        ):
                            continue
                        eligible_choices.append(choice_data)
                    if not eligible_choices:
                        continue

                    market_name = MarketRepository._normalize_market_name(
                        market_data.get("marketName")
                    )
                    if not market_name:
                        continue
                    market_group = MarketRepository._normalize_market_group(
                        market_data.get("marketGroup")
                    )
                    market_period = MarketRepository._normalize_market_period(
                        market_data.get("marketPeriod")
                    )
                    choice_group = MarketRepository._normalize_string_or_none(
                        market_data.get("choiceGroup")
                    )
                    is_live = bool(market_data.get("isLive", False))
                    identity = market_identity(
                        bookie_id,
                        market_name,
                        market_period,
                        choice_group,
                        is_live,
                    )
                    market = market_index.get(identity)
                    if market is None:
                        legacy_key = (
                            bookie_id,
                            market_group,
                            choice_group,
                            is_live,
                        )
                        market = legacy_full_time_index.pop(legacy_key, None)
                    if market is None:
                        market = Market(
                            event_id=event_id,
                            bookie_id=bookie_id,
                            market_name=market_name,
                            market_group=market_group,
                            market_period=market_period,
                            choice_group=choice_group,
                            is_live=is_live,
                            collected_at=collected_at,
                        )
                        session.add(market)
                    else:
                        market.market_name = market_name
                        market.market_group = market_group
                        market.market_period = market_period
                        market.choice_group = choice_group
                        market.collected_at = collected_at
                    market_index[identity] = market
                    prepared_markets.append((market, eligible_choices))
                    result.markets_saved += 1

            # Assign IDs to all new markets in one flush.
            session.flush()
            prepared_choices = []
            for market, eligible_choices in prepared_markets:
                existing_choices = {
                    choice.choice_name: choice
                    for choice in market.choices
                }
                for choice_data in eligible_choices:
                    choice_name = MarketRepository._normalize_string_or_none(
                        choice_data.get("name")
                    )
                    initial_odds = MarketRepository._choice_odds_value(
                        choice_data,
                        "initialFractionalValue",
                        "initialDecimalValue",
                        "initialOdds",
                        "initial_odds",
                    )
                    current_odds = MarketRepository._choice_odds_value(
                        choice_data,
                        "fractionalValue",
                        "decimalValue",
                        "currentOdds",
                        "current_odds",
                        "odds",
                    )
                    choice = existing_choices.get(choice_name)
                    if (
                        write_policy.overwrite_initial_odds
                        and initial_odds is not None
                    ):
                        effective_initial = initial_odds
                    elif choice is not None and choice.initial_odds is not None:
                        effective_initial = choice.initial_odds
                    else:
                        effective_initial = initial_odds
                    effective_current = (
                        current_odds
                        if write_policy.persist_current_odds
                        else (
                            choice.current_odds
                            if choice is not None
                            else None
                        )
                    )
                    change = MarketRepository._choice_change(
                        explicit_change=(
                            choice_data.get("change")
                            if write_policy.persist_current_odds
                            else None
                        ),
                        initial_odds=effective_initial,
                        current_odds=effective_current,
                    )
                    initial_was_set = False
                    if choice is None:
                        choice = MarketChoice(
                            market_id=market.market_id,
                            choice_name=choice_name,
                            initial_odds=initial_odds,
                            current_odds=effective_current,
                            change=change if change is not None else 0,
                        )
                        session.add(choice)
                        existing_choices[choice_name] = choice
                        initial_was_set = initial_odds is not None
                    else:
                        if (
                            write_policy.persist_current_odds
                            and current_odds is not None
                        ):
                            choice.current_odds = current_odds
                        if change is not None:
                            choice.change = change
                        if (
                            write_policy.overwrite_initial_odds
                            and initial_odds is not None
                        ):
                            existing_initial = MarketRepository._numeric_or_none(
                                choice.initial_odds
                            )
                            incoming_initial = MarketRepository._numeric_or_none(
                                initial_odds
                            )
                            if existing_initial != incoming_initial:
                                choice.initial_odds = initial_odds
                                initial_was_set = True
                        elif choice.initial_odds is None and initial_odds is not None:
                            choice.initial_odds = initial_odds
                            initial_was_set = True
                    prepared_choices.append(
                        (
                            choice,
                            choice_data,
                            effective_current,
                            initial_odds,
                            initial_was_set,
                        )
                    )
                    result.choices_saved += 1

            # Assign IDs to all new choices in one flush, then append snapshots.
            session.flush()
            for choice, choice_data, current_odds, initial_odds, initial_was_set in prepared_choices:
                initial_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("initialChangedAt")
                )
                if (
                    write_policy.persist_opening_snapshots
                    and initial_was_set
                    and initial_odds is not None
                    and initial_source_collected_at
                ):
                    session.add(
                        MarketChoiceSnapshot(
                            choice_id=choice.choice_id,
                            odds_value=initial_odds,
                            collected_at=collected_at,
                            source=source,
                            source_collected_at=initial_source_collected_at,
                        )
                    )
                    result.snapshots_saved += 1
                if (
                    write_policy.persist_current_snapshots
                    and current_odds is not None
                ):
                    session.add(
                        MarketChoiceSnapshot(
                            choice_id=choice.choice_id,
                            odds_value=current_odds,
                            collected_at=collected_at,
                            source=source,
                            source_collected_at=MarketRepository._parse_source_datetime(
                                choice_data.get("sourceCollectedAt")
                            ),
                        )
                    )
                    result.snapshots_saved += 1

        oddsportal_logger.info(
            "Saved canonical event batch: event=%s bookies=%s markets=%s "
            "choices=%s snapshots=%s policy=%s",
            event_id,
            len(bookie_ids),
            result.markets_saved,
            result.choices_saved,
            result.snapshots_saved,
            write_policy.name,
        )
        return result

    @staticmethod
    def _choice_odds_value(choice_data: Dict, fractional_key: str, *decimal_keys):
        fractional = choice_data.get(fractional_key)
        if fractional:
            decimal_value = MarketRepository._fractional_to_decimal(fractional)
            if decimal_value is not None:
                return decimal_value

        for key in decimal_keys:
            value = choice_data.get(key)
            if value is None or value == "":
                continue
            try:
                return round(float(value), 3)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _normalize_market_name(name: str) -> str:
        if name is None:
            return None
        normalized = str(name).strip()
        return normalized or None

    @staticmethod
    def _normalize_market_group(group: str) -> str:
        if group is None:
            return None
        normalized = str(group).strip()
        return normalized or None

    @staticmethod
    def get_markets_for_event(event_id: int) -> List[Market]:
        try:
            with db_manager.get_session() as session:
                markets = session.query(Market).options(
                    joinedload(Market.choices)
                ).filter(Market.event_id == event_id).all()
                return markets
        except Exception as e:
            logger.error(f"Error getting markets for event {event_id}: {e}")
            return []

    @staticmethod
    def get_external_markets_for_event(event_id: int) -> List[Dict]:
        """
        Fetch external bookmaker markets for a specific event (bookie_id != 1).
        
        Extracts market choices, odds movement, and snapshot source information
        for all non-primary bookies.
        """
        try:
            from sqlalchemy.orm import joinedload
            with db_manager.get_session() as session:
                markets = (
                    session.query(Market)
                    .options(joinedload(Market.choices), joinedload(Market.bookie))
                    .filter(
                        Market.event_id == event_id,
                        Market.bookie_id.isnot(None),
                        Market.bookie_id != 1
                    )
                    .all()
                )

                result = []
                for market in markets:
                    bookie_name = market.bookie.name if market.bookie else "Unknown"
                    choices_data = []
                    for choice in sorted(market.choices, key=lambda c: c.choice_name):
                        initial = float(choice.initial_odds) if choice.initial_odds is not None else None
                        current = float(choice.current_odds) if choice.current_odds is not None else None
                        if initial is not None and current is not None:
                            if current > initial:
                                movement = '↑'
                            elif current < initial:
                                movement = '↓'
                            else:
                                movement = '='
                        elif current is not None:
                            movement = '='
                        else:
                            movement = '='
                        choices_data.append({
                            'name': choice.choice_name,
                            'initial': initial,
                            'current': current,
                            'movement': movement
                        })

                    # Determine source from snapshots of choices
                    source = "oddsportal"
                    if market.choices:
                        first_choice = market.choices[0]
                        snapshot = (
                            session.query(MarketChoiceSnapshot)
                            .filter(MarketChoiceSnapshot.choice_id == first_choice.choice_id)
                            .order_by(MarketChoiceSnapshot.collected_at.desc())
                            .first()
                        )
                        if snapshot and snapshot.source:
                            source = snapshot.source

                    result.append({
                        'bookie_name': bookie_name,
                        'choice_group': market.choice_group,
                        'market_name': market.market_name,
                        'market_group': market.market_group,
                        'market_period': market.market_period,
                        'is_live': market.is_live,
                        'choices': choices_data,
                        'source': source
                    })

                def sort_key(m):
                    group_order = {'1X2': 1, 'Asian Handicap': 2, 'Over/Under': 3}
                    mg_order = group_order.get(m.get('market_group', ''), 4)
                    period_order = {'Full Time': 1, '1st Half': 2, '2nd Half': 3}
                    mp_order = period_order.get(m.get('market_period', ''), 4)
                    bookie_is_betfair = 1 if 'betfair' in m['bookie_name'].lower() else 0
                    cg = m.get('choice_group') or ''
                    return (mg_order, mp_order, cg, bookie_is_betfair, m['bookie_name'])

                result.sort(key=sort_key)
                return result
        except Exception as e:
            logger.error(f"Error getting external markets for event {event_id}: {e}")
            return []

    # Backwards compatibility alias
    get_oddsportal_markets_for_event = get_external_markets_for_event

    @staticmethod
    def get_market_count(event_id: int) -> int:
        try:
            with db_manager.get_session() as session:
                count = session.query(Market).filter(Market.event_id == event_id).count()
                return count
        except Exception:
            return 0

    @staticmethod
    def _normalize_market_period(period: str) -> str:
        """Apply only defensive trimming; provider semantics are normalized upstream."""
        return MarketRepository._normalize_string_or_none(period) or "Full Time"

    @staticmethod
    def _build_choice_payload(choice_name: str, current_odds, initial_odds=None) -> Dict:
        payload = {
            "name": choice_name,
            "currentOdds": current_odds,
        }
        if initial_odds is not None:
            payload["initialOdds"] = initial_odds
        return payload

    @staticmethod
    def _save_oddsportal_market(
        event_id: int,
        source_bookie_name: str,
        source_bookie_slug: str,
        market_name: str,
        market_group: Optional[str],
        market_period: Optional[str],
        choice_group: Optional[str],
        choices: List[Dict],
    ) -> int:
        resolution = BookieRepository.resolve_bookie_from_source(
            source="oddsportal",
            source_bookie_name=source_bookie_name,
            source_bookie_slug=source_bookie_slug,
            allow_create=False,
        )
        if not resolution.resolved or resolution.bookie is None:
            oddsportal_logger.warning(
                "Skipping unresolved OddsPortal bookie slug=%s name=%s",
                source_bookie_slug,
                source_bookie_name,
            )
            return 0

        odds_response = MarketRepository._build_single_market_response(
            market_name=market_name,
            market_group=market_group,
            market_period=market_period,
            choice_group=choice_group,
            choices=choices,
        )
        save_result = MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=odds_response,
            bookie_id=resolution.bookie.bookie_id,
            source="oddsportal",
        )
        return save_result.markets_saved

    @staticmethod
    def save_markets_from_oddsportal(event_id: int, odds_data: object) -> int:
        """
        Save markets from OddsPortal scraper data.

        Iterates over odds_data.extractions (list of MarketExtraction) to save
        each period's bookie odds and Betfair data with the correct
        market_group, market_period, and market_name metadata.

        Falls back to legacy bookie_odds/betfair fields if extractions is empty
        (backward compatibility with older scraper output).
        """
        try:
            if not odds_data:
                return 0

            extraction_tuples = []

            if hasattr(odds_data, 'extractions') and odds_data.extractions:
                for ext in odds_data.extractions:
                    extraction_tuples.append((
                        ext.market_group,
                        ext.market_period,
                        ext.market_name,
                        ext.bookie_odds,
                        ext.betfair,
                    ))
            elif odds_data.bookie_odds or odds_data.betfair:
                extraction_tuples.append((
                    "1X2",
                    "Full Time",
                    "Full Time",
                    odds_data.bookie_odds,
                    odds_data.betfair,
                ))

            if not extraction_tuples:
                oddsportal_logger.warning(
                    f"⚠️ save_markets_from_oddsportal called with EMPTY data for event {event_id}"
                )
                return 0

            saved_count = 0
            total_bookies = sum(len(t[3]) for t in extraction_tuples)
            total_betfair = sum(1 for t in extraction_tuples if t[4])
            oddsportal_logger.debug(
                f"💾 Saving OddsPortal data for event {event_id}: "
                f"{len(extraction_tuples)} period(s), {total_bookies} bookies, "
                f"{total_betfair} Betfair sections"
            )
            for market_group, market_period, market_name, bookie_odds_list, betfair_data in extraction_tuples:
                market_period_normalized = MarketRepository._normalize_market_period(market_period)
                is_ou = market_group == "Over/Under"
                choice_1_key = "over" if is_ou else "1"
                choice_2_key = "under" if is_ou else "2"

                for b_odds in bookie_odds_list:
                    source_bookie_name = MarketRepository._normalize_string_or_none(b_odds.name)
                    source_bookie_slug = MarketRepository._slugify_source_bookie_name(source_bookie_name)
                    if not source_bookie_name or not source_bookie_slug:
                        oddsportal_logger.warning(
                            "Skipping OddsPortal bookie with missing name/slug for event %s (%s)",
                            event_id,
                            market_name,
                        )
                        continue

                    initial_map = {
                        choice_1_key: MarketRepository._float_or_none(b_odds.initial_odds_1),
                        "x": MarketRepository._float_or_none(b_odds.initial_odds_x),
                        choice_2_key: MarketRepository._float_or_none(b_odds.initial_odds_2),
                    }
                    choices = []
                    for choice_name, raw_value in {
                        choice_1_key: b_odds.odds_1,
                        "x": b_odds.odds_x,
                        choice_2_key: b_odds.odds_2,
                    }.items():
                        current_odds = MarketRepository._float_or_none(raw_value)
                        if current_odds is None:
                            continue
                        choices.append(
                            MarketRepository._build_choice_payload(
                                choice_name,
                                current_odds,
                                initial_map.get(choice_name),
                            )
                        )

                    if not choices:
                        continue

                    handicap_normalized = MarketRepository._normalize_string_or_none(getattr(b_odds, "handicap", None))
                    saved_count += MarketRepository._save_oddsportal_market(
                        event_id=event_id,
                        source_bookie_name=source_bookie_name,
                        source_bookie_slug=source_bookie_slug,
                        market_name=market_name,
                        market_group=market_group,
                        market_period=market_period_normalized,
                        choice_group=handicap_normalized,
                        choices=choices,
                    )

                if betfair_data:
                    source_bookie_name = "Betfair Exchange"
                    source_bookie_slug = "betfair-ex"
                    exchange_configs = [
                        {
                            "group": "Back",
                            "initials": {
                                choice_1_key: MarketRepository._float_or_none(betfair_data.initial_back_1),
                                "x": MarketRepository._float_or_none(betfair_data.initial_back_x),
                                choice_2_key: MarketRepository._float_or_none(betfair_data.initial_back_2),
                            },
                            "choices": {
                                choice_1_key: betfair_data.back_1,
                                "x": betfair_data.back_x,
                                choice_2_key: betfair_data.back_2,
                            },
                        },
                        {
                            "group": "Lay",
                            "initials": {
                                choice_1_key: MarketRepository._float_or_none(betfair_data.initial_lay_1),
                                "x": MarketRepository._float_or_none(betfair_data.initial_lay_x),
                                choice_2_key: MarketRepository._float_or_none(betfair_data.initial_lay_2),
                            },
                            "choices": {
                                choice_1_key: betfair_data.lay_1,
                                "x": betfair_data.lay_x,
                                choice_2_key: betfair_data.lay_2,
                            },
                        },
                    ]

                    for config in exchange_configs:
                        choices = []
                        for choice_name, raw_value in config["choices"].items():
                            current_odds = MarketRepository._float_or_none(raw_value)
                            if current_odds is None:
                                continue
                            choices.append(
                                MarketRepository._build_choice_payload(
                                    choice_name,
                                    current_odds,
                                    config["initials"].get(choice_name),
                                )
                            )

                        if not choices:
                            continue

                        bf_choice_group = config["group"]
                        if getattr(betfair_data, "handicap", None):
                            bf_choice_group = f"{bf_choice_group} {betfair_data.handicap}"
                        bf_choice_group_normalized = MarketRepository._normalize_string_or_none(bf_choice_group)
                        saved_count += MarketRepository._save_oddsportal_market(
                            event_id=event_id,
                            source_bookie_name=source_bookie_name,
                            source_bookie_slug=source_bookie_slug,
                            market_name=market_name,
                            market_group=market_group,
                            market_period=market_period_normalized,
                            choice_group=bf_choice_group_normalized,
                            choices=choices,
                        )

            return saved_count

        except Exception as e:
            oddsportal_logger.error(
                f"Error saving OddsPortal markets for event {event_id}: {e}"
            )
            return 0

    @staticmethod
    def delete_markets_for_event(event_id: int) -> bool:
        """
        Delete all markets and choices for an event.
        """
        try:
            with db_manager.get_session() as session:
                deleted = session.query(Market).filter(Market.event_id == event_id).delete()
                session.commit()
                logger.debug(f"Deleted {deleted} markets for event {event_id}")
                return True
        except Exception as e:
            logger.error(f"Error deleting markets for event {event_id}: {e}")
            return False
