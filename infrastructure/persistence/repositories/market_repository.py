import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import datetime

from sqlalchemy import and_
from sqlalchemy.orm import selectinload

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
from infrastructure.persistence.repositories.market.market_choice_quote_writer import (
    MarketChoiceQuoteWriter,
)
from infrastructure.persistence.repositories.market.market_choice_snapshot_writer import (
    MarketChoiceSnapshotWriter,
)
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
    def _snapshot_collected_at_key(collected_at: datetime) -> datetime:
        if collected_at.tzinfo is not None:
            return collected_at.replace(microsecond=0, tzinfo=None)
        return collected_at.replace(microsecond=0)

    @staticmethod
    def _existing_snapshot_keys(session, *, event_id: int, source: str) -> set[tuple[int, datetime]]:
        rows = (
            session.query(
                MarketChoiceSnapshot.quote_id,
                MarketChoiceSnapshot.collected_at,
            )
            .join(
                MarketChoiceQuote,
                MarketChoiceSnapshot.quote_id == MarketChoiceQuote.quote_id,
            )
            .join(MarketChoice, MarketChoiceQuote.choice_id == MarketChoice.choice_id)
            .join(Market, MarketChoice.market_id == Market.market_id)
            .filter(
                Market.event_id == int(event_id),
                MarketChoiceQuote.source == source,
            )
            .all()
        )
        keys: set[tuple[int, datetime]] = set()
        for quote_id, collected_at in rows:
            if quote_id is None or collected_at is None:
                continue
            keys.add(
                (
                    int(quote_id),
                    MarketRepository._snapshot_collected_at_key(collected_at),
                )
            )
        return keys

    @staticmethod
    def _existing_moment_snapshot_source_keys(
        session,
        *,
        event_id: int,
        source: str,
    ) -> dict[tuple[int, datetime], datetime | None]:
        """Load existing moment-snapshot dedup keys keyed by source_collected_at.

        Used exclusively by the momentQuotes persist section of
        save_canonical_bookmaker_batches so that two runs for the same
        theoretical moment (same ``collected_at = start_time - delta``) are
        only considered duplicates when the bookmaker reported the same tick
        (same ``source_collected_at`` / ``createdAt``).  A price that changed
        by a few milliseconds will have a different ``source_collected_at`` and
        will therefore produce a new snapshot row.

        Returns a dict ``{(quote_id, collected_at_key): source_collected_at}``
        where ``source_collected_at`` may be ``None`` when the original row
        was persisted without a bookmaker timestamp.
        """
        rows = (
            session.query(
                MarketChoiceSnapshot.quote_id,
                MarketChoiceSnapshot.collected_at,
                MarketChoiceSnapshot.source_collected_at,
            )
            .join(
                MarketChoiceQuote,
                MarketChoiceSnapshot.quote_id == MarketChoiceQuote.quote_id,
            )
            .join(MarketChoice, MarketChoiceQuote.choice_id == MarketChoice.choice_id)
            .join(Market, MarketChoice.market_id == Market.market_id)
            .filter(
                Market.event_id == int(event_id),
                MarketChoiceQuote.source == source,
            )
            .all()
        )
        result: dict[tuple[int, datetime], datetime | None] = {}
        for quote_id, collected_at, source_collected_at in rows:
            if quote_id is None or collected_at is None:
                continue
            key = (
                int(quote_id),
                MarketRepository._snapshot_collected_at_key(collected_at),
            )
            result[key] = source_collected_at
        return result

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
                for market in market_index.values()
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
                            market,
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
            existing_snapshot_keys = set()
            moment_snapshot_source_keys: dict[tuple[int, datetime], datetime | None] = {}
            has_moment_quotes = any(
                isinstance(choice_data.get("momentQuotes"), list)
                and choice_data.get("momentQuotes")
                for _, _, choice_data, _, _, _ in prepared_choices
            )
            if has_moment_quotes:
                existing_snapshot_keys = MarketRepository._existing_snapshot_keys(
                    session,
                    event_id=event_id,
                    source=source,
                )
                moment_snapshot_source_keys = (
                    MarketRepository._existing_moment_snapshot_source_keys(
                        session,
                        event_id=event_id,
                        source=source,
                    )
                )
            for market, choice, choice_data, current_odds, initial_odds, initial_was_set in prepared_choices:
                initial_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("initialChangedAt"),
                    convert_to_project_timezone=uses_oddspapi_source_time,
                )
                current_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("sourceCollectedAt") or choice_data.get("changedAt"),
                    convert_to_project_timezone=uses_oddspapi_source_time,
                )
                if (
                    current_source_collected_at is None
                    and not uses_oddspapi_source_time
                ):
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
                    current_captured_at=collected_at,
                )

                if choice_data.get("mainLine") is True:
                    MarketRepository._demote_superseded_mainlines(
                        market=market,
                        market_index=market_index,
                        quote_index=quote_index,
                        source=source,
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
                    if ("back", 0) in explicit_exchange_quotes
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
                    if current_quote.quote_id is not None:
                        existing_snapshot_keys.add(
                            (
                                int(current_quote.quote_id),
                                MarketRepository._snapshot_collected_at_key(
                                    collected_at
                                ),
                            )
                        )

                moment_quotes = choice_data.get("momentQuotes")
                if (
                    write_policy.persist_current_snapshots
                    and isinstance(moment_quotes, list)
                    and moment_quotes
                ):
                    moment_quote_row = quotes_by_identity.get(primary_identity)
                    if moment_quote_row is None:
                        raise ValueError(
                            "Moment snapshot has no matching quote for "
                            f"choice_id={choice.choice_id}, identity={primary_identity}"
                        )
                    if moment_quote_row.quote_id is None:
                        session.flush()
                    for moment_quote in moment_quotes:
                        if not isinstance(moment_quote, dict):
                            continue
                        moment_odds = MarketRepository._float_or_none(
                            moment_quote.get("price")
                            or moment_quote.get("decimalValue")
                        )
                        moment_collected_at = moment_quote.get("collectedAt")
                        if not isinstance(moment_collected_at, datetime):
                            moment_collected_at = (
                                MarketRepository._parse_source_datetime(
                                    moment_collected_at,
                                    convert_to_project_timezone=False,
                                )
                            )
                        if moment_odds is None or moment_collected_at is None:
                            continue
                        snapshot_key = (
                            int(moment_quote_row.quote_id),
                            MarketRepository._snapshot_collected_at_key(
                                moment_collected_at
                            ),
                        )
                        if snapshot_key in moment_snapshot_source_keys:
                            # A snapshot for this theoretical moment already exists.
                            # Only skip if the bookmaker timestamp is identical
                            # (same tick = same price). A different source_collected_at
                            # means the bookmaker updated the price; insert a new row.
                            incoming_src_ts = MarketRepository._parse_source_datetime(
                                moment_quote.get("createdAt"),
                                convert_to_project_timezone=uses_oddspapi_source_time,
                            )
                            existing_src_ts = moment_snapshot_source_keys[snapshot_key]
                            if (
                                incoming_src_ts is None
                                or existing_src_ts is None
                                or incoming_src_ts == existing_src_ts
                            ):
                                # No bookmaker timestamp available on one side,
                                # or both timestamps match: same tick, skip.
                                continue
                            # Different source_collected_at: the book moved, persist.
                        elif snapshot_key in existing_snapshot_keys:
                            # Covered by the regular (non-moment) snapshot dedup.
                            continue
                        incoming_src_ts_for_write = MarketRepository._parse_source_datetime(
                            moment_quote.get("createdAt"),
                            convert_to_project_timezone=uses_oddspapi_source_time,
                        )
                        MarketChoiceSnapshotWriter.append(
                            session,
                            quote=moment_quote_row,
                            odds_value=moment_odds,
                            collected_at=moment_collected_at,
                            source_collected_at=incoming_src_ts_for_write,
                            source_limit=MarketRepository._numeric_or_none(
                                moment_quote.get("limit")
                            ),
                            exchange_size=(
                                MarketRepository._numeric_or_none(
                                    moment_quote.get("limit")
                                )
                                if moment_quote_row.exchange_side is not None
                                else None
                            ),
                        )
                        existing_snapshot_keys.add(snapshot_key)
                        moment_snapshot_source_keys[snapshot_key] = incoming_src_ts_for_write
                        result.snapshots_saved += 1

            # Persist the complete quote/snapshot graph in one flush. Snapshot
            # relationships can reference pending quotes; SQLAlchemy orders the
            # INSERTs by FK dependency without a per-choice round trip.
            session.flush()

        operation_logger.info(
            "✅ Saved canonical event batch: event=%s input_bookies=%s "
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
    def _demote_superseded_mainlines(
        *,
        market: Market,
        market_index: dict,
        quote_index: dict,
        source: str,
    ) -> None:
        """Demote main_line to False on previous lines for the same bookie/market/period."""
        if market.choice_group is None:
            return

        for other_market in market_index.values():
            if other_market is market or (
                other_market.market_id is not None
                and other_market.market_id == market.market_id
            ):
                continue
            if (
                other_market.bookie_id == market.bookie_id
                and other_market.market_name == market.market_name
                and other_market.market_period == market.market_period
                and other_market.is_live == market.is_live
                and other_market.choice_group != market.choice_group
            ):
                for other_choice in other_market.choices:
                    if other_choice.choice_id is None:
                        continue
                    for (c_id, src, _, _), quote in quote_index.items():
                        if c_id == other_choice.choice_id and src == source:
                            if getattr(quote, "main_line", None) is True:
                                quote.main_line = False

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
          carrying explicit sides for one outcome. A valid top back quote is
          the canonical primary price, so no redundant side-agnostic row is
          written; each valid entry gets its own row.

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
                explicit_change=choice_data.get("change"),
                overwrite_initial=write_policy.overwrite_initial_odds,
                **common_source_fields,
            )
            if upsert_result is not None and upsert_result.quote is not None:
                quotes_by_identity[(explicit_side, 0)] = upsert_result.quote
            return quotes_by_identity

        exchange_quotes = choice_data.get("exchangeQuotes")
        normalized_exchange_quotes = []
        if isinstance(exchange_quotes, list):
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
                normalized_exchange_quotes.append(
                    (quote, quote_price, quote_side, quote_level)
                )

        has_top_back = any(
            quote_side == "back" and quote_level == 0
            for _, _, quote_side, quote_level in normalized_exchange_quotes
        )
        if not has_top_back:
            upsert_result = MarketChoiceQuoteWriter.upsert(
                session,
                quote_index=quote_index,
                choice_id=choice.choice_id,
                source=source,
                initial_price=initial_odds,
                initial_captured_at=initial_captured_at,
                current_price=(
                    current_odds if write_policy.persist_current_odds else None
                ),
                current_captured_at=current_captured_at,
                source_limit=MarketRepository._numeric_or_none(
                    choice_data.get("limit")
                ),
                explicit_change=choice_data.get("change"),
                overwrite_initial=write_policy.overwrite_initial_odds,
                **common_source_fields,
            )
            if upsert_result is not None and upsert_result.quote is not None:
                quotes_by_identity[(None, 0)] = upsert_result.quote

        if not normalized_exchange_quotes:
            return quotes_by_identity
        exchange_current_captured_at = (
            MarketRepository._resolve_exchange_observation_time(
                current_odds=current_odds,
                initial_captured_at=initial_captured_at,
                current_captured_at=current_captured_at,
            )
        )
        for quote, quote_price, quote_side, quote_level in normalized_exchange_quotes:
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
                explicit_change=choice_data.get("change"),
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
                try:
                    level = int(quote.get("level"))
                except (TypeError, ValueError):
                    level = 0
                if (
                    side == "back"
                    and level == 0
                    and MarketRepository._float_or_none(quote.get("price")) is not None
                ):
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
    def get_external_markets_for_event(event_id: int):
        """Return the canonical quote-aware external market blocks."""
        from infrastructure.persistence.repositories.market.market_quote_read_policy import (
            load_quote_read_priority_policy,
        )
        from infrastructure.persistence.repositories.market.market_read_queries import (
            MarketReadQueries,
        )
        from infrastructure.settings import Config

        policy = load_quote_read_priority_policy(Config.ODDS_READ_PRIORITY_CONFIG)
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

    @staticmethod
    def has_external_markets_for_event(event_id: int) -> bool:
        """Check availability through the canonical quote-aware reader."""
        from infrastructure.persistence.repositories.market.market_read_queries import (
            MarketReadQueries,
        )
        return MarketReadQueries.has_external_market_quotes_for_event(event_id)

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
