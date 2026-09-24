import logging
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from sqlalchemy import and_
from sqlalchemy.orm import selectinload

from infrastructure.persistence.models import (
    CanonicalMarketType,
    Market,
    MarketChoice,
    MarketChoiceQuote,
    MarketChoiceSnapshot,
)
from infrastructure.persistence.database import db_manager
from infrastructure.persistence.catalogs.canonical_market_types import (
    CANONICAL_MARKET_TYPE_IDS,
    CANONICAL_MARKET_TYPE_SEEDS,
    persisted_seed_values,
)
from infrastructure.persistence.repositories.event_source_mapping_repository import (
    EventSourceMappingRepository,
)
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
from shared.temporal import as_utc, utc_now

logger = logging.getLogger(__name__)
oddsportal_logger = logging.LoggerAdapter(logger, {"oddsportal": True})


@dataclass
class MarketSaveResult:
    markets_saved: int = 0
    choices_saved: int = 0
    snapshots_saved: int = 0
    quotes_persisted: int = 0


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
        return as_utc(parsed, field_name="market source timestamp")

    @staticmethod
    def _requires_provider_timestamp(source: str | None) -> bool:
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
    def _normalize_line_value(value) -> Optional[Decimal]:
        """Normalize a market line to a finite signed decimal.

        Empty values are valid for non-line markets.  Invalid non-empty values
        are rejected at the repository boundary instead of becoming a second
        textual market identity.
        """
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        try:
            normalized = Decimal(str(value).strip())
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError(f"invalid numeric line value: {value!r}")
        if not normalized.is_finite():
            raise ValueError(f"invalid numeric line value: {value!r}")
        return normalized

    @staticmethod
    def _event_source_mapping_source(quote_source: str) -> str | None:
        """Map quote persistence sources to their event mapping source key."""
        normalized = str(quote_source or "").strip().lower()
        if normalized.startswith("sofascore"):
            return "sofascore"
        if normalized.startswith("oddspapi"):
            return "oddspapi"
        if normalized.startswith("oddsportal"):
            return "oddsportal"
        return None

    @staticmethod
    def _market_line_value(market) -> Optional[Decimal]:
        """Read the canonical numeric line value."""
        return MarketRepository._normalize_line_value(market.line_value)

    @staticmethod
    def _float_or_none(value):
        normalized = normalize_odds_value(value)
        return float(normalized) if normalized is not None else None

    @staticmethod
    def _snapshot_collected_at_key(collected_at: datetime) -> datetime:
        return as_utc(
            collected_at,
            field_name="snapshot collected_at",
        ).replace(microsecond=0)

    @staticmethod
    def _existing_moment_snapshot_source_keys(
        session,
        *,
        event_id: int,
        source: str,
        earliest: datetime,
        latest: datetime,
    ) -> dict[tuple[int, datetime], set[datetime | None]]:
        """Load all source ticks per second, restricted to the incoming interval.

        A single value per key loses earlier ticks within the same second and
        makes their next replay insert duplicates. None remains a wildcard for
        legacy rows without provider timestamps.
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
                MarketChoiceSnapshot.collected_at >= earliest,
                MarketChoiceSnapshot.collected_at < latest + timedelta(seconds=1),
            )
            .yield_per(1000)
        )
        result: dict[tuple[int, datetime], set[datetime | None]] = {}
        for quote_id, collected_at, source_collected_at in rows:
            if quote_id is None or collected_at is None:
                continue
            key = (
                int(quote_id),
                MarketRepository._snapshot_collected_at_key(collected_at),
            )
            result.setdefault(key, set()).add(source_collected_at)
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
        collected_at = utc_now()
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

            canonical_keys = {
                str(market_data.get("canonicalMarketKey") or "").strip().lower()
                for batch in batches
                for market_data in (batch.get("markets") or [])
                if str(market_data.get("canonicalMarketKey") or "").strip()
            }
            canonical_types_by_key = {}
            canonical_types = []
            if canonical_keys:
                canonical_types_by_key = {
                    row.canonical_market_key: row
                    for row in (
                        session.query(CanonicalMarketType)
                        .filter(CanonicalMarketType.canonical_market_key.in_(canonical_keys))
                        .all()
                    )
                }
                # create_tables() is intentionally lighter than the startup
                # migration in tests and local tooling. Materialize only known
                # catalog keys in this same transaction so canonical writes do
                # not silently fall back to provider display text.
                for canonical_key in canonical_keys - canonical_types_by_key.keys():
                    seed = CANONICAL_MARKET_TYPE_SEEDS.get(canonical_key)
                    market_type_id = CANONICAL_MARKET_TYPE_IDS.get(canonical_key)
                    if seed is None or market_type_id is None:
                        continue
                    canonical_type = CanonicalMarketType(
                        canonical_market_key=canonical_key,
                        market_type_id=market_type_id,
                        **persisted_seed_values(seed),
                    )
                    session.add(canonical_type)
                    canonical_types_by_key[canonical_key] = canonical_type
            canonical_types = session.query(CanonicalMarketType).all()
            if not canonical_types and not canonical_keys:
                for canonical_key, seed in CANONICAL_MARKET_TYPE_SEEDS.items():
                    market_type_id = CANONICAL_MARKET_TYPE_IDS.get(canonical_key)
                    if market_type_id is None:
                        continue
                    row = CanonicalMarketType(
                        canonical_market_key=canonical_key,
                        market_type_id=market_type_id,
                        **persisted_seed_values(seed),
                    )
                    session.add(row)
                    canonical_types.append(row)

            def resolve_catalog_type(market_data):
                canonical_key = str(
                    market_data.get("canonicalMarketKey") or ""
                ).strip().lower()
                if canonical_key:
                    return canonical_types_by_key.get(canonical_key)
                normalized = tuple(
                    str(market_data.get(field) or "").strip().casefold()
                    for field in ("marketName", "marketGroup", "marketPeriod")
                )
                matches = [
                    row
                    for row in canonical_types
                    if (
                        row.canonical_market_name.strip().casefold(),
                        row.canonical_market_group.strip().casefold(),
                        row.canonical_market_period.strip().casefold(),
                    )
                    == normalized
                ]
                return matches[0] if len(matches) == 1 else None

            def canonical_market_identity(
                bookie_id,
                market_type_id,
                line_value,
                is_live,
            ):
                return (
                    int(bookie_id),
                    int(market_type_id),
                    MarketRepository._normalize_line_value(line_value),
                    bool(is_live),
                )

            canonical_market_index = {
                canonical_market_identity(
                    market.bookie_id,
                    market.market_type_id,
                    MarketRepository._market_line_value(market),
                    market.is_live,
                ): market
                for market in existing_markets
                if market.market_type_id is not None
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
                    canonical_market_key = str(
                        market_data.get("canonicalMarketKey") or ""
                    ).strip().lower()
                    canonical_market_type = resolve_catalog_type(market_data)
                    if canonical_market_type is not None and not canonical_market_key:
                        canonical_market_key = canonical_market_type.canonical_market_key
                    if canonical_market_type is None:
                        skipped_market_count += 1
                        operation_logger.error(
                            "Canonical persistence skipped market: event=%s source=%s "
                            "bookie_id=%s canonical_market_key=%s reason=unknown_canonical_market_key",
                            event_id,
                            source,
                            bookie_id,
                            canonical_market_key,
                        )
                        continue

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

                    raw_line_value = market_data.get("lineValue")
                    try:
                        line_value = MarketRepository._normalize_line_value(
                            raw_line_value
                        )
                    except ValueError:
                        skipped_market_count += 1
                        operation_logger.error(
                            "Canonical persistence skipped market: event=%s source=%s "
                            "bookie_id=%s canonical_market_key=%s "
                            "reason=invalid_numeric_line_value value=%r",
                            event_id,
                            source,
                            bookie_id,
                            canonical_market_key,
                            raw_line_value,
                        )
                        continue
                    if (
                        canonical_market_type is not None
                        and canonical_market_type.requires_line_value
                        and line_value is None
                    ):
                        skipped_market_count += 1
                        operation_logger.error(
                            "Canonical persistence skipped market: event=%s source=%s "
                            "bookie_id=%s canonical_market_key=%s "
                            "reason=required_line_value_missing",
                            event_id,
                            source,
                            bookie_id,
                            canonical_market_key,
                        )
                        continue
                    is_live = bool(market_data.get("isLive", False))
                    market_type_id = (
                        int(canonical_market_type.market_type_id)
                        if canonical_market_type is not None
                        else None
                    )
                    canonical_identity = (
                        canonical_market_identity(
                            bookie_id,
                            market_type_id,
                            line_value,
                            is_live,
                        )
                        if market_type_id is not None
                        else None
                    )
                    market = (
                        canonical_market_index.get(canonical_identity)
                        if canonical_identity is not None
                        else None
                    )
                    if (
                        market is not None
                        and market_type_id is not None
                        and market.market_type_id not in {None, market_type_id}
                    ):
                        skipped_market_count += 1
                        operation_logger.error(
                            "Canonical persistence skipped conflicting market identity: "
                            "event=%s source=%s market_id=%s existing_market_type_id=%s "
                            "incoming_market_type_id=%s",
                            event_id,
                            source,
                            market.market_id,
                            market.market_type_id,
                            market_type_id,
                        )
                        continue
                    if market is None:
                        market = Market(
                            event_id=event_id,
                            bookie_id=bookie_id,
                            market_type_id=market_type_id,
                            line_value=line_value,
                            is_live=is_live,
                            collected_at=collected_at,
                        )
                        session.add(market)
                    else:
                        if market_type_id is not None:
                            market.market_type_id = market_type_id
                        market.line_value = line_value
                        market.collected_at = collected_at
                    if canonical_identity is not None:
                        canonical_market_index[canonical_identity] = market
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
                for market in existing_markets
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
            requires_provider_timestamp = MarketRepository._requires_provider_timestamp(
                source
            )
            existing_snapshot_keys = set()
            moment_snapshot_source_keys: dict[
                tuple[int, datetime], set[datetime | None]
            ] = {}
            earliest_moment = latest_moment = None
            for _, _, choice_data, _, _, _ in prepared_choices:
                moments = choice_data.get("momentQuotes")
                if not isinstance(moments, list):
                    continue
                for moment in moments:
                    if not isinstance(moment, dict):
                        continue
                    moment_at = MarketRepository._parse_source_datetime(
                        moment.get("collectedAt")
                    )
                    if moment_at is None:
                        continue
                    moment_at = MarketRepository._snapshot_collected_at_key(moment_at)
                    earliest_moment = (
                        moment_at if earliest_moment is None
                        else min(earliest_moment, moment_at)
                    )
                    latest_moment = (
                        moment_at if latest_moment is None
                        else max(latest_moment, moment_at)
                    )
            if earliest_moment is not None:
                moment_snapshot_source_keys = (
                    MarketRepository._existing_moment_snapshot_source_keys(
                        session,
                        event_id=event_id,
                        source=source,
                        earliest=earliest_moment,
                        latest=latest_moment,
                    )
                )
            for market, choice, choice_data, current_odds, initial_odds, initial_was_set in prepared_choices:
                initial_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("initialChangedAt"),
                )
                current_source_collected_at = MarketRepository._parse_source_datetime(
                    choice_data.get("sourceCollectedAt") or choice_data.get("changedAt"),
                )
                if (
                    current_source_collected_at is None
                    and not requires_provider_timestamp
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
                # A returned quote row means this ingest supplied an initial
                # or current price candidate that is represented in storage.
                result.quotes_persisted += len(quotes_by_identity)
                persist_current_snapshot = (
                    choice_data.get("persistCurrentSnapshot", True) is not False
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
                        if (
                            not persist_current_snapshot
                            and (quote_side, quote_level) == primary_identity
                        ):
                            continue
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
                    and persist_current_snapshot
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
                        incoming_src_ts_for_write = MarketRepository._parse_source_datetime(
                            moment_quote.get("createdAt"),
                        )
                        if snapshot_key in moment_snapshot_source_keys:
                            # A snapshot for this theoretical moment already exists.
                            # Only skip if the bookmaker timestamp is identical
                            # (same tick = same price). A different source_collected_at
                            # means the bookmaker updated the price; insert a new row.
                            existing_src_times = moment_snapshot_source_keys[snapshot_key]
                            if (
                                incoming_src_ts_for_write is None
                                or None in existing_src_times
                                or incoming_src_ts_for_write in existing_src_times
                            ):
                                # No bookmaker timestamp available on one side,
                                # or both timestamps match: same tick, skip.
                                continue
                            # Different source_collected_at: the book moved, persist.
                        elif snapshot_key in existing_snapshot_keys:
                            # Covered by the regular (non-moment) snapshot dedup.
                            continue
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
                        moment_snapshot_source_keys.setdefault(snapshot_key, set()).add(
                            incoming_src_ts_for_write
                        )
                        result.snapshots_saved += 1

            MarketRepository._demote_superseded_mainlines(
                market_index=canonical_market_index,
                quote_index=quote_index,
                prepared_choices=prepared_choices,
                source=source,
            )

            if result.quotes_persisted:
                mapping_source = MarketRepository._event_source_mapping_source(
                    source
                )
                if mapping_source is not None:
                    EventSourceMappingRepository.mark_odds_available(
                        [int(event_id)],
                        mapping_source,
                        session=session,
                    )

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
        market_index: dict,
        quote_index: dict,
        prepared_choices: list,
        source: str,
    ) -> None:
        """Reconcile mainlines once per batch, including choices created this ingest.

        The provider adapter owns selection. If a caller supplies several selected
        lines, preserve that ambiguity instead of inventing an order-based winner.
        All state is already loaded; this is linear work with no extra queries.
        """
        def family(market):
            return (market.bookie_id, market.market_type_id, market.is_live)

        market_by_choice = {
            choice.choice_id: market
            for market in market_index.values()
            for choice in market.choices
        }
        selected_lines: dict[tuple, set] = {}
        for market, choice, choice_data, *_ in prepared_choices:
            market_by_choice[choice.choice_id] = market
            current_line = MarketRepository._market_line_value(market)
            if current_line is not None and choice_data.get("mainLine") is True:
                selected_lines.setdefault(family(market), set()).add(current_line)

        for (choice_id, quote_source, _, _), quote in quote_index.items():
            market = market_by_choice.get(choice_id)
            if quote_source != source or market is None or quote.main_line is not True:
                continue
            selected = selected_lines.get(family(market), set())
            if len(selected) == 1 and MarketRepository._market_line_value(market) not in selected:
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
