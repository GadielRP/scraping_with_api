import logging
import re
import time
import zlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.orm import joinedload, selectinload

from infrastructure.persistence.models import (
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.market_write_policy import (
    market_write_policy_for_source,
)
from infrastructure.persistence.repositories.bookie_repository import BookieRepository
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)
from infrastructure.persistence.repositories.market.market_choice_snapshot_writer import (
    MarketChoiceSnapshotWriter,
)
from infrastructure.persistence.repositories.market.odds_movement import compute_movement
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
        """Thin wrapper kept for internal call sites; logic lives in odds_movement.

        See infrastructure/persistence/repositories/market/odds_movement.py
        (extracted per docs/refactors/db-schema-odds-refactor.md §7) so
        MarketChoiceQuoteWriter can reuse the exact same computation.
        """
        return compute_movement(
            explicit_change=explicit_change,
            initial_odds=initial_odds,
            current_odds=current_odds,
        )

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

    # LEGACY_MAINTENANCE_ONLY: live ingestion uses save_canonical_bookmaker_batches.
    # Migrate the remaining maintenance scripts and delete this facade in Fase 8.
    @staticmethod
    def save_markets_from_response(event_id: int, odds_response: Dict, bookie_id: int) -> int:
        """Save all markets from an odds API response to the database.

        LEGACY_MAINTENANCE_ONLY: as of docs/refactors/db-schema-odds-refactor.md
        (Fase 2), the live ingestion pipeline (market_odds_ingestion_service.py)
        persists OddsPortal, OddsPapi and SofaScore exclusively through
        `save_canonical_bookmaker_batches`, which also writes MarketChoiceQuote.
        This method (and `save_markets_from_response_with_stats` below) is not
        dead: it's still called by scripts/legacy/process_null_seasons_legacy_event_odds.py,
        scripts/legacy/extract_historical_results_legacy_event_odds.py and
        scripts/sport_seasons_processing.py. Do not add new call sites; migrate
        those scripts and remove both methods in Fase 8.
        """
        return MarketRepository.save_markets_from_response_with_stats(
            event_id=event_id,
            odds_response=odds_response,
            bookie_id=bookie_id,
            source="sofascore",
        ).markets_saved

    # LEGACY_MAINTENANCE_ONLY: duplicated market/choice orchestration retained only
    # for explicitly listed maintenance scripts. Do not add call sites; Fase 8 deletes it.
    @staticmethod
    def save_markets_from_response_with_stats(
        event_id: int,
        odds_response: Dict,
        bookie_id: int,
        source: str,
    ) -> MarketSaveResult:
        source = str(source or "").strip().lower()
        if not source:
            raise ValueError("source is required to persist quote-linked snapshots")
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
                existing_quotes = (
                    session.query(MarketChoiceQuote)
                    .join(
                        MarketChoice,
                        MarketChoice.choice_id == MarketChoiceQuote.choice_id,
                    )
                    .join(Market, Market.market_id == MarketChoice.market_id)
                    .filter(
                        Market.event_id == event_id,
                        Market.bookie_id == bookie_id,
                        MarketChoiceQuote.source == source,
                    )
                    .all()
                )
                quote_index = {
                    MarketChoiceQuoteWriter.identity_key(
                        choice_id=quote.choice_id,
                        source=quote.source,
                        exchange_side=quote.exchange_side,
                        exchange_level=quote.exchange_level,
                    ): quote
                    for quote in existing_quotes
                }
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
                                initial_source_collected_at = (
                                    MarketRepository._parse_source_datetime(
                                        choice_data.get("initialChangedAt"),
                                        convert_to_project_timezone=uses_oddspapi_source_time,
                                    )
                                )
                                quotes_by_identity = MarketRepository._upsert_choice_quotes(
                                    session,
                                    quote_index=quote_index,
                                    choice=choice,
                                    choice_data=choice_data,
                                    source=source,
                                    write_policy=write_policy,
                                    initial_odds=initial_odds,
                                    initial_captured_at=initial_source_collected_at,
                                    current_odds=current_odds,
                                    current_captured_at=source_collected_at,
                                )

                                exchange_quotes = choice_data.get("exchangeQuotes")
                                explicit_exchange_quotes = {
                                    identity: quote
                                    for identity, quote in quotes_by_identity.items()
                                    if identity[0] is not None
                                }
                                explicit_side = str(
                                    choice_data.get("exchangeSide") or ""
                                ).strip().lower()
                                opening_identity = (
                                    ("back", 0)
                                    if explicit_exchange_quotes
                                    and isinstance(exchange_quotes, list)
                                    else (
                                        (explicit_side, 0)
                                        if explicit_side in {"back", "lay"}
                                        else (None, 0)
                                    )
                                )
                                if (
                                    write_policy.persist_opening_snapshots
                                    and initial_was_set
                                    and initial_odds is not None
                                    and initial_source_collected_at is not None
                                ):
                                    opening_quote = quotes_by_identity.get(
                                        opening_identity
                                    )
                                    if opening_quote is None:
                                        raise ValueError(
                                            "Opening snapshot has no matching quote for "
                                            f"choice_id={choice.choice_id}, identity={opening_identity}"
                                        )
                                    initial_limit = MarketRepository._numeric_or_none(
                                        choice_data.get("initialLimit")
                                    )
                                    MarketChoiceSnapshotWriter.append(
                                        session,
                                        quote=opening_quote,
                                        odds_value=initial_odds,
                                        collected_at=market.collected_at,
                                        source_collected_at=initial_source_collected_at,
                                        source_limit=initial_limit,
                                        exchange_size=(
                                            initial_limit
                                            if opening_quote.exchange_side is not None
                                            else None
                                        ),
                                    )
                                    result.snapshots_saved += 1

                                if (
                                    write_policy.persist_current_snapshots
                                    and explicit_exchange_quotes
                                    and isinstance(exchange_quotes, list)
                                ):
                                    for quote_data in exchange_quotes:
                                        if not isinstance(quote_data, dict):
                                            continue
                                        quote_price = MarketRepository._float_or_none(
                                            quote_data.get("price")
                                        )
                                        quote_side = str(
                                            quote_data.get("side") or ""
                                        ).strip().lower()
                                        try:
                                            quote_level = int(quote_data.get("level"))
                                        except (TypeError, ValueError):
                                            continue
                                        if quote_price is None or quote_side not in {"back", "lay"}:
                                            continue
                                        persisted_quote = quotes_by_identity.get(
                                            (quote_side, quote_level)
                                        )
                                        if persisted_quote is None:
                                            raise ValueError(
                                                "Current exchange snapshot has no matching quote for "
                                                f"choice_id={choice.choice_id}, "
                                                f"identity={(quote_side, quote_level)}"
                                            )

                                        MarketChoiceSnapshotWriter.append(
                                            session,
                                            quote=persisted_quote,
                                            odds_value=quote_price,
                                            collected_at=market.collected_at,
                                            source_collected_at=source_collected_at,
                                            source_limit=(
                                                MarketRepository._numeric_or_none(
                                                    choice_data.get("limit")
                                                )
                                            ),
                                            exchange_size=(
                                                MarketRepository._numeric_or_none(
                                                    quote_data.get("size")
                                                )
                                            ),
                                        )
                                        result.snapshots_saved += 1
                                elif (
                                    write_policy.persist_current_snapshots
                                    and effective_current_odds is not None
                                ):
                                    current_quote = quotes_by_identity.get(
                                        opening_identity
                                    )
                                    if current_quote is None:
                                        raise ValueError(
                                            "Current snapshot has no matching quote for "
                                            f"choice_id={choice.choice_id}, identity={opening_identity}"
                                        )
                                    MarketChoiceSnapshotWriter.append(
                                        session,
                                        quote=current_quote,
                                        odds_value=effective_current_odds,
                                        collected_at=market.collected_at,
                                        source_collected_at=source_collected_at,
                                        source_limit=MarketRepository._numeric_or_none(
                                            choice_data.get("limit")
                                        ),
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
        source = str(source or "").strip().lower()
        if not source:
            raise ValueError("source is required to persist canonical quotes")
        write_policy = market_write_policy_for_source(source)
        operation_logger = (
            oddsportal_logger
            if source == "oddsportal" or source.startswith("oddsportal_")
            else logger
        )
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
        persisted_bookie_ids = set()
        skipped_market_count = 0
        skipped_choice_count = 0
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
                source_bookie_name = (
                    MarketRepository._normalize_string_or_none(
                        batch.get("source_bookie_name")
                    )
                    or "unknown"
                )
                source_bookie_slug = (
                    MarketRepository._normalize_string_or_none(
                        batch.get("source_bookie_slug")
                    )
                    or "unknown"
                )
                for market_data in batch.get("markets") or []:
                    eligible_choices = []
                    seen_choice_sides = set()
                    missing_initial_choices = []
                    for choice_data in market_data.get("choices") or []:
                        choice_name = MarketRepository._normalize_string_or_none(
                            choice_data.get("name")
                        )
                        # Betfair Exchange sends back AND lay as two choice
                        # dicts sharing the same name within one market (see
                        # OddsPortalMarketAdapter._build_betfair_exchange_markets,
                        # Fase 3). Dedupe on (name, exchangeSide) so lay isn't
                        # silently dropped as a "duplicate" of back.
                        choice_side = str(choice_data.get("exchangeSide") or "").strip().lower()
                        dedupe_key = (choice_name, choice_side)
                        if not choice_name or dedupe_key in seen_choice_sides:
                            continue
                        seen_choice_sides.add(dedupe_key)
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
                            skipped_choice_count += 1
                            missing_initial_choices.append(
                                {
                                    "choice": choice_name,
                                    "exchange_side": choice_side or None,
                                    "current_odds": MarketRepository._choice_odds_value(
                                        choice_data,
                                        "fractionalValue",
                                        "decimalValue",
                                        "currentOdds",
                                        "current_odds",
                                        "odds",
                                    ),
                                }
                            )
                            continue
                        eligible_choices.append(choice_data)
                    if missing_initial_choices:
                        operation_logger.warning(
                            "Canonical persistence rejected choices: event=%s "
                            "source=%s bookmaker=%s bookmaker_slug=%s bookie_id=%s "
                            "market=%s period=%s reason=required_initial_odds_missing "
                            "rejected=%s policy=%s",
                            event_id,
                            source,
                            source_bookie_name,
                            source_bookie_slug,
                            bookie_id,
                            market_data.get("marketName"),
                            market_data.get("marketPeriod"),
                            missing_initial_choices,
                            write_policy.name,
                        )
                    if not eligible_choices:
                        skipped_market_count += 1
                        operation_logger.warning(
                            "Canonical persistence skipped market: event=%s source=%s "
                            "bookmaker=%s bookmaker_slug=%s bookie_id=%s market=%s "
                            "period=%s reason=no_choices_satisfied_write_policy "
                            "input_choices=%s policy=%s",
                            event_id,
                            source,
                            source_bookie_name,
                            source_bookie_slug,
                            bookie_id,
                            market_data.get("marketName"),
                            market_data.get("marketPeriod"),
                            len(market_data.get("choices") or []),
                            write_policy.name,
                        )
                        continue

                    market_name = MarketRepository._normalize_market_name(
                        market_data.get("marketName")
                    )
                    if not market_name:
                        skipped_market_count += 1
                        operation_logger.warning(
                            "Canonical persistence skipped market: event=%s source=%s "
                            "bookmaker=%s bookmaker_slug=%s bookie_id=%s "
                            "reason=market_name_missing policy=%s",
                            event_id,
                            source,
                            source_bookie_name,
                            source_bookie_slug,
                            bookie_id,
                            write_policy.name,
                        )
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
                    persisted_bookie_ids.add(bookie_id)
                    result.markets_saved += 1

            # Assign IDs to all new markets in one flush.
            session.flush()

            # MarketChoice is pure identity (market_id, choice_name) as of this
            # refactor - initial_odds/current_odds/change are no longer written
            # here. MarketChoiceQuote (per source/side/level) is the sole
            # persistence target for price state; see
            # docs/refactors/db-schema-odds-refactor.md §3.2 (accepted risk:
            # non-migrated readers see incomplete data until Fase 5).
            # "Was this choice's opening price set for the first time (or
            # legitimately overwritten)" - the signal that gates whether we
            # append an opening MarketChoiceSnapshot - is looked up from the
            # existing *quote of the same side/level* (via quote_index), not
            # from a NULL-side-only map and not from the frozen choice mirror.
            existing_choice_ids = [
                choice.choice_id
                for market, _ in prepared_markets
                for choice in market.choices
                if choice.choice_id is not None
            ]
            quote_index = {}
            if existing_choice_ids:
                existing_quotes = (
                    session.query(MarketChoiceQuote)
                    .filter(
                        MarketChoiceQuote.choice_id.in_(existing_choice_ids),
                        MarketChoiceQuote.source == source,
                    )
                    .all()
                )
                quote_index = {
                    MarketChoiceQuoteWriter.identity_key(
                        choice_id=quote.choice_id,
                        source=quote.source,
                        exchange_side=quote.exchange_side,
                        exchange_level=quote.exchange_level,
                    ): quote
                    for quote in existing_quotes
                }

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

                    if choice is None:
                        choice = MarketChoice(
                            market_id=market.market_id,
                            choice_name=choice_name,
                        )
                        session.add(choice)
                        existing_choices[choice_name] = choice
                        initial_was_set = initial_odds is not None
                    elif choice.choice_id is None:
                        # Same MarketChoice created earlier in this batch
                        # (OddsPortal back+lay share one choice_name) — not
                        # flushed yet, so no quote row can exist in quote_index.
                        initial_was_set = initial_odds is not None
                    else:
                        gate_side, gate_level = (
                            MarketRepository._opening_gate_side_and_level(
                                choice_data
                            )
                        )
                        existing_quote = quote_index.get(
                            MarketChoiceQuoteWriter.identity_key(
                                choice_id=choice.choice_id,
                                source=source,
                                exchange_side=gate_side,
                                exchange_level=gate_level,
                            )
                        )
                        existing_initial = MarketRepository._numeric_or_none(
                            existing_quote.initial_odds
                            if existing_quote is not None
                            else None
                        )
                        incoming_initial = MarketRepository._numeric_or_none(
                            initial_odds
                        )
                        if (
                            write_policy.overwrite_initial_odds
                            and incoming_initial is not None
                        ):
                            initial_was_set = existing_initial != incoming_initial
                        else:
                            initial_was_set = (
                                existing_initial is None
                                and incoming_initial is not None
                            )

                    prepared_choices.append(
                        (
                            choice,
                            choice_data,
                            current_odds,
                            initial_odds,
                            initial_was_set,
                        )
                    )
                    result.choices_saved += 1

            # Assign IDs to all new choices in one flush, then append snapshots
            # and refresh the current-state MarketChoiceQuote cache.
            session.flush()
            uses_oddspapi_source_time = MarketRepository._uses_utc_source_timestamps(
                source
            )
            for choice, choice_data, current_odds, initial_odds, initial_was_set in prepared_choices:
                initial_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("initialChangedAt"),
                    convert_to_project_timezone=uses_oddspapi_source_time,
                )
                current_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("sourceCollectedAt") or choice_data.get("changedAt"),
                    convert_to_project_timezone=uses_oddspapi_source_time,
                )
                # Providers without a source clock (SofaScore) use extraction
                # time. Matches the quote contract: current_updated_at prefers
                # source time and falls back to collected_at.
                if current_source_collected_at is None:
                    current_source_collected_at = collected_at
                quotes_by_identity = MarketRepository._upsert_choice_quotes(
                    session,
                    quote_index=quote_index,
                    choice=choice,
                    choice_data=choice_data,
                    source=source,
                    write_policy=write_policy,
                    initial_odds=initial_odds,
                    initial_captured_at=initial_source_collected_at,
                    current_odds=current_odds,
                    current_captured_at=current_source_collected_at,
                )

                exchange_quotes = choice_data.get("exchangeQuotes")
                explicit_exchange_quotes = {
                    identity: quote
                    for identity, quote in quotes_by_identity.items()
                    if identity[0] is not None
                }
                explicit_side = str(
                    choice_data.get("exchangeSide") or ""
                ).strip().lower()
                primary_identity = (
                    ("back", 0)
                    if explicit_exchange_quotes and isinstance(exchange_quotes, list)
                    else (
                        (explicit_side, 0)
                        if explicit_side in {"back", "lay"}
                        else (None, 0)
                    )
                )
                if (
                    write_policy.persist_opening_snapshots
                    and initial_was_set
                    and initial_odds is not None
                    and initial_source_collected_at
                ):
                    opening_quote = quotes_by_identity.get(primary_identity)
                    if opening_quote is None:
                        raise ValueError(
                            "Opening snapshot has no matching quote for "
                            f"choice_id={choice.choice_id}, identity={primary_identity}"
                        )
                    initial_limit = MarketRepository._numeric_or_none(
                        choice_data.get("initialLimit")
                    )
                    MarketChoiceSnapshotWriter.append(
                        session,
                        quote=opening_quote,
                        odds_value=initial_odds,
                        collected_at=collected_at,
                        source_collected_at=initial_source_collected_at,
                        source_limit=initial_limit,
                        exchange_size=(
                            initial_limit
                            if opening_quote.exchange_side is not None
                            else None
                        ),
                    )
                    result.snapshots_saved += 1

                if (
                    write_policy.persist_current_snapshots
                    and explicit_exchange_quotes
                    and isinstance(exchange_quotes, list)
                ):
                    for quote_data in exchange_quotes:
                        if not isinstance(quote_data, dict):
                            continue
                        quote_price = MarketRepository._float_or_none(
                            quote_data.get("price")
                        )
                        quote_side = str(
                            quote_data.get("side") or ""
                        ).strip().lower()
                        try:
                            quote_level = int(quote_data.get("level"))
                        except (TypeError, ValueError):
                            continue
                        if quote_price is None or quote_side not in {"back", "lay"}:
                            continue
                        persisted_quote = quotes_by_identity.get(
                            (quote_side, quote_level)
                        )
                        if persisted_quote is None:
                            raise ValueError(
                                "Current exchange snapshot has no matching quote for "
                                f"choice_id={choice.choice_id}, "
                                f"identity={(quote_side, quote_level)}"
                            )
                        MarketChoiceSnapshotWriter.append(
                            session,
                            quote=persisted_quote,
                            odds_value=quote_price,
                            collected_at=collected_at,
                            source_collected_at=(
                                MarketRepository._resolve_exchange_observation_time(
                                    current_odds=current_odds,
                                    initial_captured_at=initial_source_collected_at,
                                    current_captured_at=current_source_collected_at,
                                )
                            ),
                            source_limit=MarketRepository._numeric_or_none(
                                choice_data.get("limit")
                            ),
                            exchange_size=MarketRepository._numeric_or_none(
                                quote_data.get("size")
                            ),
                        )
                        result.snapshots_saved += 1
                elif (
                    write_policy.persist_current_snapshots
                    and current_odds is not None
                ):
                    current_quote = quotes_by_identity.get(primary_identity)
                    if current_quote is None:
                        raise ValueError(
                            "Current snapshot has no matching quote for "
                            f"choice_id={choice.choice_id}, identity={primary_identity}"
                        )
                    MarketChoiceSnapshotWriter.append(
                        session,
                        quote=current_quote,
                        odds_value=current_odds,
                        collected_at=collected_at,
                        source_collected_at=current_source_collected_at,
                        source_limit=MarketRepository._numeric_or_none(
                            choice_data.get("limit")
                        ),
                    )
                    result.snapshots_saved += 1

            # Persist the complete quote/snapshot graph in one flush. Snapshot
            # relationships can reference pending quotes; SQLAlchemy orders the
            # INSERTs by FK dependency without a per-choice round trip.
            session.flush()

        operation_logger.info(
            "Saved canonical event batch: event=%s input_bookies=%s "
            "persisted_bookies=%s markets=%s choices=%s snapshots=%s "
            "skipped_markets=%s skipped_choices=%s policy=%s",
            event_id,
            len(bookie_ids),
            len(persisted_bookie_ids),
            result.markets_saved,
            result.choices_saved,
            result.snapshots_saved,
            skipped_market_count,
            skipped_choice_count,
            write_policy.name,
        )
        return result

    @staticmethod
    def _resolve_exchange_observation_time(
        *,
        current_odds,
        initial_captured_at,
        current_captured_at,
    ):
        """Timestamp a ladder that accompanied opening-only data as opening."""
        if current_odds is None and initial_captured_at is not None:
            return initial_captured_at
        return current_captured_at

    @staticmethod
    def _upsert_choice_quotes(
        session,
        *,
        quote_index,
        choice,
        choice_data: Dict,
        source: str,
        write_policy,
        initial_odds,
        initial_captured_at,
        current_odds,
        current_captured_at,
    ) -> Dict[tuple[Optional[str], int], MarketChoiceQuote]:
        """Persist current quote state and return it by exact side/level identity.

        Three shapes of choice_data are handled:
        - Plain single-price choice (most bookies): writes a side-agnostic
          row (exchange_side=None) with the effective initial/current prices
          (policy-gated).
        - OddsPortal Betfair Exchange: choice_data['exchangeSide'] names the
          single side ('back'/'lay') this choice_data dict already IS -
          initial_odds/current_odds are that side's own values, so they are
          written straight to that side's quote instead of also to the
          side-agnostic row (there is no side-agnostic price to mirror there).
        - Oddspapi Betfair Exchange: choice_data['exchangeQuotes'] is a list
          carrying BOTH sides (and price levels) for one outcome, alongside a
          side-agnostic price; each entry gets its own row.

        LEGACY note (pre-§3.2 wording): older comments described the
        side-agnostic quote as "mirroring MarketChoice.initial_odds/
        current_odds". That mirror is gone — the canonical path no longer
        writes those MarketChoice columns; MarketChoiceQuote is the sole
        persistence target for price state. See
        docs/refactors/db-schema-odds-refactor.md §3.2 and Fase 2-3.
        """
        source = str(source or "").strip().lower()
        if not source:
            raise ValueError("source is required to persist market choice quotes")

        quotes_by_identity = {}
        common_source_fields = dict(
            main_line=choice_data.get("mainLine"),
            source_market_id=choice_data.get("sourceMarketId"),
            source_outcome_id=choice_data.get("sourceOutcomeId"),
            bookmaker_outcome_id=choice_data.get("bookmakerOutcomeId"),
        )

        explicit_side = str(choice_data.get("exchangeSide") or "").strip().lower()
        if explicit_side in {"back", "lay"}:
            upsert_result = MarketChoiceQuoteWriter.upsert(
                session,
                quote_index=quote_index,
                choice_id=choice.choice_id,
                source=source,
                exchange_side=explicit_side,
                exchange_level=0,
                initial_price=initial_odds,
                initial_captured_at=initial_captured_at,
                current_price=current_odds if write_policy.persist_current_odds else None,
                current_captured_at=current_captured_at,
                source_limit=MarketRepository._numeric_or_none(choice_data.get("limit")),
                overwrite_initial=write_policy.overwrite_initial_odds,
                **common_source_fields,
            )
            if upsert_result is not None and upsert_result.quote is not None:
                quotes_by_identity[(explicit_side, 0)] = upsert_result.quote
            return quotes_by_identity

        upsert_result = MarketChoiceQuoteWriter.upsert(
            session,
            quote_index=quote_index,
            choice_id=choice.choice_id,
            source=source,
            initial_price=initial_odds,
            initial_captured_at=initial_captured_at,
            current_price=current_odds if write_policy.persist_current_odds else None,
            current_captured_at=current_captured_at,
            source_limit=MarketRepository._numeric_or_none(choice_data.get("limit")),
            overwrite_initial=write_policy.overwrite_initial_odds,
            **common_source_fields,
        )
        if upsert_result is not None and upsert_result.quote is not None:
            quotes_by_identity[(None, 0)] = upsert_result.quote

        exchange_quotes = choice_data.get("exchangeQuotes")
        if not isinstance(exchange_quotes, list):
            return quotes_by_identity
        exchange_current_captured_at = (
            MarketRepository._resolve_exchange_observation_time(
                current_odds=current_odds,
                initial_captured_at=initial_captured_at,
                current_captured_at=current_captured_at,
            )
        )
        for quote in exchange_quotes:
            if not isinstance(quote, dict):
                continue
            quote_price = MarketRepository._float_or_none(quote.get("price"))
            quote_side = str(quote.get("side") or "").strip().lower()
            if quote_price is None or quote_side not in {"back", "lay"}:
                continue
            try:
                quote_level = int(quote.get("level"))
            except (TypeError, ValueError):
                quote_level = 0

            # Only the top-of-book back quote carries a meaningful "opening"
            # value today (mirrors the legacy MarketChoiceSnapshot behaviour
            # of labelling the choice-level initial_odds as exchange_side
            # "back"); lay has no historical opening counterpart yet.
            side_initial_price = (
                initial_odds if quote_side == "back" and quote_level == 0 else None
            )
            side_initial_captured_at = (
                initial_captured_at if side_initial_price is not None else None
            )

            upsert_result = MarketChoiceQuoteWriter.upsert(
                session,
                quote_index=quote_index,
                choice_id=choice.choice_id,
                source=source,
                exchange_side=quote_side,
                exchange_level=quote_level,
                initial_price=side_initial_price,
                initial_captured_at=side_initial_captured_at,
                current_price=quote_price,
                current_captured_at=exchange_current_captured_at,
                source_limit=MarketRepository._numeric_or_none(quote.get("size")),
                overwrite_initial=write_policy.overwrite_initial_odds,
                **common_source_fields,
            )
            if upsert_result is not None and upsert_result.quote is not None:
                quotes_by_identity[(quote_side, quote_level)] = upsert_result.quote

        return quotes_by_identity

    @staticmethod
    def _opening_gate_side_and_level(choice_data: Dict) -> tuple[Optional[str], int]:
        """Return the quote side/level whose initial gates opening snapshots.

        Must match the identity used later for the opening snapshot itself:
        - OddsPortal Betfair: choice_data['exchangeSide'] (back|lay)
        - Oddspapi Betfair with exchangeQuotes: top back (level 0), same as
          primary_identity when building opening snapshots
        - Everyone else: side-agnostic NULL / level 0

        Lookup uses the already-preloaded quote_index — no extra DB query.
        """
        explicit_side = str(choice_data.get("exchangeSide") or "").strip().lower()
        if explicit_side in {"back", "lay"}:
            return explicit_side, 0

        exchange_quotes = choice_data.get("exchangeQuotes")
        if isinstance(exchange_quotes, list):
            for quote in exchange_quotes:
                if not isinstance(quote, dict):
                    continue
                side = str(quote.get("side") or "").strip().lower()
                if side in {"back", "lay"}:
                    return "back", 0

        return None, 0

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

    # LEGACY_ODDS_READ: infers source from snapshots and reads frozen choice odds.
    # Retained only for shadow/rollback during Phase 5; remove in Phase 8.
    @staticmethod
    def _get_external_markets_legacy(event_id: int) -> List[Dict]:
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

    @staticmethod
    def _is_shadow_sampled(event_id: int, sample_rate: float) -> bool:
        if sample_rate <= 0:
            return False
        if sample_rate >= 1:
            return True
        bucket = zlib.crc32(str(int(event_id)).encode("ascii")) % 10_000
        return bucket < int(sample_rate * 10_000)

    @staticmethod
    def get_external_markets_for_event(event_id: int):
        """Read external markets through the Phase 5 cutover facade.

        ``legacy`` returns the historical dict contract, ``shadow`` returns the
        same contract while comparing quotes, and ``quotes`` returns typed
        ``ExternalMarketQuoteBlock`` instances.
        """
        from infrastructure.persistence.repositories.market.market_quote_read_policy import (
            load_quote_read_priority_policy,
        )
        from infrastructure.persistence.repositories.market.market_read_comparator import (
            compare_external_market_reads,
        )
        from infrastructure.persistence.repositories.market.market_read_queries import (
            MarketReadQueries,
        )
        from infrastructure.settings import Config

        mode = Config.EXTERNAL_ODDS_READ_MODE
        if mode == "legacy":
            return MarketRepository._get_external_markets_legacy(event_id)

        policy = load_quote_read_priority_policy(Config.ODDS_READ_PRIORITY_CONFIG)
        if mode == "quotes":
            result = MarketReadQueries.get_external_market_quotes_for_event(event_id, policy)
            blocking = [item.code for item in result.diagnostics if item.blocking]
            if blocking:
                logger.error(
                    "Quote-aware external odds read produced blocking diagnostics "
                    "event_id=%s codes=%s",
                    event_id,
                    sorted(set(blocking)),
                )
            return list(result.blocks)

        legacy_started = time.perf_counter()
        legacy = MarketRepository._get_external_markets_legacy(event_id)
        legacy_duration_ms = (time.perf_counter() - legacy_started) * 1000
        if not MarketRepository._is_shadow_sampled(
            event_id, Config.ODDS_READ_SHADOW_SAMPLE_RATE
        ):
            return legacy

        quote_started = time.perf_counter()
        result = MarketReadQueries.get_external_market_quotes_for_event(event_id, policy)
        quote_duration_ms = (time.perf_counter() - quote_started) * 1000
        comparison = compare_external_market_reads(
            event_id=event_id,
            legacy_markets=legacy,
            quote_blocks=result.blocks,
            quote_diagnostics=result.diagnostics,
            legacy_stop_write_at=Config.MARKET_CHOICE_LEGACY_STOP_WRITE_AT,
            legacy_duration_ms=legacy_duration_ms,
            quote_duration_ms=quote_duration_ms,
        )
        fields = comparison.as_log_fields()
        if result.has_blocking_diagnostics or comparison.blocking_count:
            logger.warning("Quote read shadow mismatch: %s", fields)
        else:
            logger.info("Quote read shadow comparison: %s", fields)
        return legacy

    @staticmethod
    def has_external_markets_for_event(event_id: int) -> bool:
        """Cheap availability check matching the active read mode."""
        from infrastructure.persistence.repositories.market.market_read_queries import (
            MarketReadQueries,
        )
        from infrastructure.settings import Config

        if Config.EXTERNAL_ODDS_READ_MODE == "quotes":
            return MarketReadQueries.has_external_market_quotes_for_event(event_id)
        with db_manager.get_session() as session:
            return (
                session.query(Market.market_id)
                .filter(
                    Market.event_id == int(event_id),
                    Market.bookie_id.isnot(None),
                    Market.bookie_id != 1,
                )
                .first()
                is not None
            )

    # LEGACY_DEAD_CODE: backwards-compat alias, no call sites found repo-wide.
    # Scheduled for removal in Fase 8. Ver docs/refactors/db-schema-odds-refactor.md §8.
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

    # LEGACY_DEAD_CODE: sin call sites activos (solo desde _save_oddsportal_market,
    # también legacy). Ver docs/refactors/db-schema-odds-refactor.md §8 Fase 8.
    @staticmethod
    def _build_choice_payload(choice_name: str, current_odds, initial_odds=None) -> Dict:
        payload = {
            "name": choice_name,
            "currentOdds": current_odds,
        }
        if initial_odds is not None:
            payload["initialOdds"] = initial_odds
        return payload

    # LEGACY_DEAD_CODE: sin call sites activos (solo desde save_markets_from_oddsportal,
    # también legacy; el path vivo de OddsPortal es OddsPortalMarketAdapter +
    # save_canonical_bookmaker_batches). Ver docs/refactors/db-schema-odds-refactor.md §8 Fase 8.
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

    # LEGACY_DEAD_CODE: no call sites found repo-wide as of refactor/db-schema-odds-refactor
    # (superseded by OddsPortalMarketAdapter.from_match_odds_data + save_canonical_bookmaker_batches,
    # wired through MarketOddsIngestionService.save_from_oddsportal_data). Scheduled for
    # removal in Fase 8. Ver docs/refactors/db-schema-odds-refactor.md §3 y §8.
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
