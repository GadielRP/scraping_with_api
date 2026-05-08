import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from sqlalchemy import and_
from sqlalchemy.orm import Session

from infrastructure.persistence.models import Bookie, Market, MarketChoice, MarketChoiceSnapshot
from infrastructure.persistence.database import db_manager
from shared.timezone_utils import get_local_now

try:
    from modules.oddsportal.oddsportal_config import BOOKIE_ALIASES
except ImportError:
    BOOKIE_ALIASES = {}

logger = logging.getLogger(__name__)


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
        try:
            if not fractional or '/' not in fractional:
                return None

            numerator, denominator = fractional.split('/')
            return round(float(numerator) / float(denominator) + 1, 3)
        except (ValueError, ZeroDivisionError):
            return None

    @staticmethod
    def save_markets_from_response(event_id: int, odds_response: Dict, bookie_id: int) -> int:
        """
        Save all markets from an odds API response to the database.
        """
        try:
            if bookie_id is None:
                logger.error("Cannot save markets for event %s without an explicit bookie_id", event_id)
                return 0

            markets_data = odds_response.get('markets', [])
            if not markets_data:
                logger.debug(f"No markets in odds response for event {event_id}")
                return 0

            saved_count = 0

            with db_manager.get_session() as session:
                for market_data in markets_data:
                    try:
                        with session.begin_nested():
                            market_name = MarketRepository._normalize_market_name(market_data.get('marketName'))
                            market_group = MarketRepository._normalize_market_group(market_data.get('marketGroup'))
                            market_period = MarketRepository._normalize_market_period(market_data.get('marketPeriod'))
                            choice_group = market_data.get('choiceGroup')
                            is_live = market_data.get('isLive', False)

                            if not market_name:
                                logger.info("Skipping market for event %s because marketName is missing", event_id)
                                continue

                            market_collected_at = get_local_now()
                            existing_market = session.query(Market).filter(
                                and_(
                                    Market.event_id == event_id,
                                    Market.bookie_id == bookie_id,
                                    Market.market_name == market_name,
                                    Market.market_period == market_period,
                                    Market.choice_group == choice_group,
                                    Market.is_live == is_live
                                )
                            ).first()

                            if existing_market:
                                market = existing_market
                                market.market_group = market_group
                                market.market_period = market_period
                                market.collected_at = market_collected_at
                            else:
                                market = Market(
                                    event_id=event_id,
                                    bookie_id=bookie_id,
                                    market_name=market_name,
                                    market_group=market_group,
                                    market_period=market_period,
                                    choice_group=choice_group,
                                    is_live=is_live,
                                    collected_at=market_collected_at
                                )
                                session.add(market)
                                session.flush()

                            choices_data = market_data.get('choices', [])
                            seen_choice_names = {}
                            for choice_data in choices_data:
                                choice_name = choice_data.get('name')
                                if choice_name and choice_name not in seen_choice_names:
                                    seen_choice_names[choice_name] = choice_data

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
                                change = choice_data.get('change', 0)

                                existing_choice = session.query(MarketChoice).filter(
                                    and_(
                                        MarketChoice.market_id == market.market_id,
                                        MarketChoice.choice_name == choice_name
                                    )
                                ).first()

                                if existing_choice:
                                    if current_odds is not None:
                                        existing_choice.current_odds = current_odds
                                    existing_choice.change = change
                                    if existing_choice.initial_odds is None and initial_odds is not None:
                                        existing_choice.initial_odds = initial_odds
                                    choice = existing_choice
                                else:
                                    choice = MarketChoice(
                                        market_id=market.market_id,
                                        choice_name=choice_name,
                                        initial_odds=initial_odds,
                                        current_odds=current_odds,
                                        change=change
                                    )
                                    session.add(choice)
                                    session.flush()

                                if current_odds is not None:
                                    snapshot = MarketChoiceSnapshot(
                                        choice_id=choice.choice_id,
                                        odds_value=current_odds,
                                        collected_at=market.collected_at
                                    )
                                    session.add(snapshot)

                            saved_count += 1
                    except Exception as e:
                        logger.warning(f"Error processing market for event {event_id}: {e}")
                        continue

                session.commit()
                logger.info(f"✅ Saved {saved_count} markets for event {event_id}")
                return saved_count

        except Exception as e:
            logger.error(f"Error saving markets for event {event_id}: {e}")
            return 0

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
                from sqlalchemy.orm import joinedload
                markets = session.query(Market).options(
                    joinedload(Market.choices)
                ).filter(Market.event_id == event_id).all()
                return markets
        except Exception as e:
            logger.error(f"Error getting markets for event {event_id}: {e}")
            return []

    @staticmethod
    def get_oddsportal_markets_for_event(event_id: int) -> List[Dict]:
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

                    result.append({
                        'bookie_name': bookie_name,
                        'choice_group': market.choice_group,
                        'market_name': market.market_name,
                        'market_group': market.market_group,
                        'market_period': market.market_period,
                        'is_live': market.is_live,
                        'choices': choices_data
                    })

                def sort_key(m):
                    group_order = {'1X2': 1, 'Asian Handicap': 2, 'Over/Under': 3}
                    mg_order = group_order.get(m.get('market_group', ''), 4)
                    period_order = {'Full-time': 1, '1st half': 2, '2nd half': 3}
                    mp_order = period_order.get(m.get('market_period', ''), 4)
                    bookie_is_betfair = 1 if 'betfair' in m['bookie_name'].lower() else 0
                    cg = m.get('choice_group') or ''
                    return (mg_order, mp_order, cg, bookie_is_betfair, m['bookie_name'])

                result.sort(key=sort_key)
                return result
        except Exception as e:
            logger.error(f"Error getting OddsPortal markets for event {event_id}: {e}")
            return []

    @staticmethod
    def get_market_count(event_id: int) -> int:
        try:
            with db_manager.get_session() as session:
                count = session.query(Market).filter(Market.event_id == event_id).count()
                return count
        except Exception:
            return 0

    @staticmethod
    def _get_or_create_bookie(session: Session, name: str) -> Bookie:
        db_name = BOOKIE_ALIASES.get(name, name)

        bookie = session.query(Bookie).filter(Bookie.name == db_name).first()
        if bookie:
            return bookie

        slug = db_name.lower().replace(' ', '-').replace('.', '')
        bookie = session.query(Bookie).filter(Bookie.slug == slug).first()
        if bookie:
            return bookie

        bookie = Bookie(name=db_name, slug=slug)
        session.add(bookie)
        session.flush()
        return bookie

    @staticmethod
    def _normalize_market_period(period: str) -> str:
        """
        Normalize OddsPortal market period strings to the canonical DB value.

        All full-time variants are collapsed to 'Full-time'. Other period
        strings (e.g. '1st half') are returned unchanged after stripping.
        """
        if period is None:
            return None

        normalized = str(period).strip()
        if not normalized:
            return None

        full_time_variants = {
            "Full Time",
            "Full time",
            "Full-time",
            "Fulltime",
            "FT",
        }

        if normalized in full_time_variants:
            return "Full-time"

        return normalized

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
                    "Full-time",
                    "Full-time",
                    odds_data.bookie_odds,
                    odds_data.betfair,
                ))

            if not extraction_tuples:
                logger.warning(f"⚠️ save_markets_from_oddsportal called with EMPTY data for event {event_id}")
                return 0

            saved_count = 0
            total_bookies = sum(len(t[3]) for t in extraction_tuples)
            total_betfair = sum(1 for t in extraction_tuples if t[4])
            logger.debug(f"💾 Saving OddsPortal data for event {event_id}: {len(extraction_tuples)} period(s), {total_bookies} bookies, {total_betfair} Betfair sections")

            with db_manager.get_session() as session:
                for market_group, market_period, market_name, bookie_odds_list, betfair_data in extraction_tuples:
                    market_period = MarketRepository._normalize_market_period(market_period)
                    for b_odds in bookie_odds_list:
                        try:
                            bookie = MarketRepository._get_or_create_bookie(session, b_odds.name)

                            market = session.query(Market).filter(
                                 and_(
                                     Market.event_id == event_id,
                                     Market.bookie_id == bookie.bookie_id,
                                     Market.market_name == market_name,
                                     Market.market_period == market_period,
                                     Market.choice_group == getattr(b_odds, 'handicap', None),
                                     Market.is_live == False
                                 )
                            ).first()

                            if not market:
                                market = Market(
                                    event_id=event_id,
                                    bookie_id=bookie.bookie_id,
                                    market_name=market_name,
                                    market_group=market_group,
                                    market_period=market_period,
                                    choice_group=getattr(b_odds, 'handicap', None),
                                    is_live=False,
                                    collected_at=get_local_now()
                                )
                                session.add(market)
                                session.flush()
                            else:
                                market.market_group = market_group
                                market.market_period = market_period
                                market.collected_at = get_local_now()

                            is_ou = market_group == "Over/Under"
                            choice_1_key = "Over" if is_ou else "1"
                            choice_2_key = "Under" if is_ou else "2"

                            initial_map = {
                                choice_1_key: None,
                                "X": None,
                                choice_2_key: None
                            }
                            try:
                                if b_odds.initial_odds_1:
                                    initial_map[choice_1_key] = float(b_odds.initial_odds_1)
                            except (ValueError, TypeError):
                                pass
                            try:
                                if b_odds.initial_odds_x:
                                    initial_map["X"] = float(b_odds.initial_odds_x)
                            except (ValueError, TypeError):
                                pass
                            try:
                                if b_odds.initial_odds_2:
                                    initial_map[choice_2_key] = float(b_odds.initial_odds_2)
                            except (ValueError, TypeError):
                                pass

                            choices_map = {
                                choice_1_key: b_odds.odds_1,
                                "X": b_odds.odds_x,
                                choice_2_key: b_odds.odds_2
                            }

                            for choice_name, val_str in choices_map.items():
                                if not val_str or val_str == '-':
                                    continue

                                try:
                                    current_odds = float(val_str)
                                except ValueError:
                                    continue

                                choice = session.query(MarketChoice).filter(
                                    and_(
                                        MarketChoice.market_id == market.market_id,
                                        MarketChoice.choice_name == choice_name
                                    )
                                ).first()

                                if choice:
                                    if abs(float(choice.current_odds or 0) - current_odds) > 0.001:
                                        choice.change = 1 if current_odds > float(choice.current_odds or 0) else -1
                                        choice.current_odds = current_odds
                                    if initial_map.get(choice_name) and not choice.initial_odds:
                                        choice.initial_odds = initial_map[choice_name]
                                else:
                                    init_val = initial_map.get(choice_name)
                                    if init_val and abs(init_val - current_odds) > 0.001:
                                        computed_change = 1 if current_odds > init_val else -1
                                    else:
                                        computed_change = 0
                                    choice = MarketChoice(
                                        market_id=market.market_id,
                                        choice_name=choice_name,
                                        initial_odds=init_val,
                                        current_odds=current_odds,
                                        change=computed_change
                                    )
                                    session.add(choice)
                                    session.flush()

                                snapshot = MarketChoiceSnapshot(
                                    choice_id=choice.choice_id,
                                    odds_value=current_odds,
                                    collected_at=market.collected_at
                                )
                                session.add(snapshot)

                            saved_count += 1

                        except Exception as e:
                            logger.warning(f"Error saving bookie {b_odds.name} for event {event_id} ({market_period}): {e}")
                            continue

                    if betfair_data:
                        try:
                            bookie = MarketRepository._get_or_create_bookie(session, "Betfair Exchange")

                            is_ou = market_group == "Over/Under"
                            choice_1_key = "Over" if is_ou else "1"
                            choice_2_key = "Under" if is_ou else "2"

                            exchange_configs = [
                                {
                                    "group": "Back",
                                    "choices": {
                                        choice_1_key: betfair_data.back_1,
                                        "X": betfair_data.back_x,
                                        choice_2_key: betfair_data.back_2
                                    }
                                },
                                {
                                    "group": "Lay",
                                    "choices": {
                                        choice_1_key: betfair_data.lay_1,
                                        "X": betfair_data.lay_x,
                                        choice_2_key: betfair_data.lay_2
                                    }
                                }
                            ]

                            for config in exchange_configs:
                                group_name = config["group"]
                                choices_map = config["choices"]

                                if not any(v and v != '-' and v.strip() for v in choices_map.values()):
                                    continue

                                bf_cg = group_name
                                if getattr(betfair_data, 'handicap', None):
                                    bf_cg = f"{group_name} {betfair_data.handicap}"

                                market = session.query(Market).filter(
                                     and_(
                                         Market.event_id == event_id,
                                         Market.bookie_id == bookie.bookie_id,
                                         Market.market_name == market_name,
                                         Market.market_period == market_period,
                                         Market.choice_group == bf_cg,
                                         Market.is_live == False
                                     )
                                ).first()

                                if not market:
                                    market = Market(
                                        event_id=event_id,
                                        bookie_id=bookie.bookie_id,
                                        market_name=market_name,
                                        market_group=market_group,
                                        market_period=market_period,
                                        choice_group=bf_cg,
                                        is_live=False,
                                        collected_at=get_local_now()
                                    )
                                    session.add(market)
                                    session.flush()
                                else:
                                    market.market_group = market_group
                                    market.market_period = market_period
                                    market.collected_at = get_local_now()

                                for choice_name, val_str in choices_map.items():
                                     if not val_str or val_str == '-' or not val_str.strip():
                                         continue
                                     try:
                                         current_odds = float(val_str)
                                     except ValueError:
                                         continue

                                     choice = session.query(MarketChoice).filter(
                                        and_(
                                            MarketChoice.market_id == market.market_id,
                                            MarketChoice.choice_name == choice_name
                                        )
                                    ).first()

                                     if choice:
                                        if abs(float(choice.current_odds or 0) - current_odds) > 0.001:
                                            choice.change = 1 if current_odds > float(choice.current_odds or 0) else -1
                                            choice.current_odds = current_odds
                                     else:
                                        bf = betfair_data
                                        if group_name == "Back":
                                            bf_initial_map = {
                                                choice_1_key: bf.initial_back_1,
                                                "X": bf.initial_back_x,
                                                choice_2_key: bf.initial_back_2
                                            }
                                        elif group_name == "Lay":
                                            bf_initial_map = {
                                                choice_1_key: bf.initial_lay_1,
                                                "X": bf.initial_lay_x,
                                                choice_2_key: bf.initial_lay_2
                                            }
                                        else:
                                            bf_initial_map = {}
                                        init_str = bf_initial_map.get(choice_name)
                                        try:
                                            init_val = float(init_str) if init_str else None
                                        except (ValueError, TypeError):
                                            init_val = None
                                        if init_val and abs(init_val - current_odds) > 0.001:
                                            computed_change = 1 if current_odds > init_val else -1
                                        else:
                                            computed_change = 0
                                        choice = MarketChoice(
                                            market_id=market.market_id,
                                            choice_name=choice_name,
                                            initial_odds=init_val,
                                            current_odds=current_odds,
                                            change=computed_change
                                        )
                                        session.add(choice)
                                        session.flush()

                                     snapshot = MarketChoiceSnapshot(
                                         choice_id=choice.choice_id,
                                         odds_value=current_odds,
                                         collected_at=market.collected_at
                                     )
                                     session.add(snapshot)

                                saved_count += 1

                        except Exception as e:
                            logger.warning(f"Error saving Betfair Exchange for event {event_id} ({market_period}): {e}")

                session.commit()
                return saved_count

        except Exception as e:
            logger.error(f"Error saving OddsPortal markets for event {event_id}: {e}")
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
