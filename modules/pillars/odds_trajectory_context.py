from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from infrastructure.settings import Config


def _coerce_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    if not text.strip():
        return None
    return value if isinstance(value, str) else text


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_decimal(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        if isinstance(value, float):
            return Decimal(str(value))
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return None
            return Decimal(stripped)
        return Decimal(str(value))
    except (ArithmeticError, InvalidOperation, ValueError, TypeError):
        return None


def _coerce_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        normalized = stripped.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _datetime_sort_value(value: Optional[datetime]) -> float:
    if value is None:
        return float("-inf")
    try:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).timestamp()
        return value.timestamp()
    except (OverflowError, OSError, ValueError):
        return float("-inf")


def _candidate_rank(meta: "OddsPointMeta") -> tuple[float, float, float]:
    distance_rank = float("inf") if meta.distance_from_target is None else float(meta.distance_from_target)
    collected_at_rank = -_datetime_sort_value(meta.collected_at)
    snapshot_rank = float("inf") if meta.snapshot_id is None else float(-meta.snapshot_id)
    return distance_rank, collected_at_rank, snapshot_rank


def _is_better_candidate(candidate: "OddsPointMeta", current: "OddsPointMeta") -> bool:
    return _candidate_rank(candidate) < _candidate_rank(current)


def _normalize_choice_group_key(choice_group: Optional[str]) -> str:
    if choice_group is None:
        return "__default__"
    if not str(choice_group).strip():
        return "__default__"
    return choice_group


def _normalize_expected_minutes(target_minutes_expected: Optional[List[int]]) -> List[int]:
    source = Config.PRE_START_ODDS_MOMENTS if target_minutes_expected is None else target_minutes_expected
    normalized: List[int] = []
    seen: set[int] = set()
    for item in source:
        minute = _coerce_int(item)
        if minute is None or minute in seen:
            continue
        seen.add(minute)
        normalized.append(minute)
    return normalized


@dataclass(frozen=True)
class OddsPointMeta:
    snapshot_id: Optional[int]
    collected_at: Optional[datetime]
    minutes_before_start: Optional[int]
    target_minute: int
    distance_from_target: Optional[int]


@dataclass(frozen=True)
class ChoiceOddsTrajectory:
    choice_name: str
    choice_id: Optional[int]
    initial_odds: Optional[Decimal]
    odds_values: Dict[int, Decimal] = field(default_factory=dict)
    meta_by_minute: Dict[int, OddsPointMeta] = field(default_factory=dict)


@dataclass(frozen=True)
class BookieOddsTrajectory:
    bookie_id: Optional[int]
    bookie_name: str
    choices: Dict[str, ChoiceOddsTrajectory] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketLineOddsTrajectory:
    market_id: Optional[int]
    market_name: str
    market_group: str
    market_period: str
    choice_group: Optional[str]
    bookies: Dict[str, BookieOddsTrajectory] = field(default_factory=dict)


@dataclass(frozen=True)
class OddsTrajectoryContext:
    available: bool
    event_id: Optional[int]
    target_minutes_expected: List[int]
    target_minutes_present: List[int]
    missing_target_minutes: List[int]
    markets: Dict[str, Dict[str, Dict[str, Dict[str, MarketLineOddsTrajectory]]]] = field(default_factory=dict)


def _get_market_line_container(
    markets: Dict[str, Dict[str, Dict[str, Dict[str, MarketLineOddsTrajectory]]]],
    market_group: str,
    market_period: str,
    market_name: str,
    choice_group_key: str,
    market_id: Optional[int],
) -> MarketLineOddsTrajectory:
    market_groups = markets.setdefault(market_group, {})
    market_periods = market_groups.setdefault(market_period, {})
    market_names = market_periods.setdefault(market_name, {})
    market_line = market_names.get(choice_group_key)
    if market_line is None:
        market_line = MarketLineOddsTrajectory(
            market_id=None,
            market_name=market_name,
            market_group=market_group,
            market_period=market_period,
            choice_group=None if choice_group_key == "__default__" else choice_group_key,
        )
        market_names[choice_group_key] = market_line
    elif market_line.market_id is None and market_id is not None:
        market_line = MarketLineOddsTrajectory(
            market_id=market_id,
            market_name=market_line.market_name,
            market_group=market_line.market_group,
            market_period=market_line.market_period,
            choice_group=market_line.choice_group,
            bookies=market_line.bookies,
        )
        market_names[choice_group_key] = market_line
    return market_line


def _get_bookie_container(
    market_line: MarketLineOddsTrajectory,
    bookie_key: str,
    bookie_id: Optional[int],
    bookie_name: str,
) -> BookieOddsTrajectory:
    bookie = market_line.bookies.get(bookie_key)
    if bookie is None:
        bookie = BookieOddsTrajectory(
            bookie_id=bookie_id,
            bookie_name=bookie_name,
        )
        market_line.bookies[bookie_key] = bookie
    elif bookie.bookie_id is None and bookie_id is not None:
        bookie = BookieOddsTrajectory(
            bookie_id=bookie_id,
            bookie_name=bookie.bookie_name,
            choices=bookie.choices,
        )
        market_line.bookies[bookie_key] = bookie
    return bookie


def _get_choice_container(
    bookie: BookieOddsTrajectory,
    choice_name: str,
    choice_id: Optional[int],
    initial_odds: Optional[Decimal],
) -> ChoiceOddsTrajectory:
    choice = bookie.choices.get(choice_name)
    if choice is None:
        choice = ChoiceOddsTrajectory(
            choice_name=choice_name,
            choice_id=choice_id,
            initial_odds=initial_odds,
        )
        bookie.choices[choice_name] = choice
    elif (choice.choice_id is None and choice_id is not None) or (
        choice.initial_odds is None and initial_odds is not None
    ):
        choice = ChoiceOddsTrajectory(
            choice_name=choice.choice_name,
            choice_id=choice_id if choice.choice_id is None and choice_id is not None else choice.choice_id,
            initial_odds=initial_odds if choice.initial_odds is None and initial_odds is not None else choice.initial_odds,
            odds_values=choice.odds_values,
            meta_by_minute=choice.meta_by_minute,
        )
        bookie.choices[choice_name] = choice
    return choice


def build_odds_trajectory_context(
    odds_trajectory: Optional[List[Dict[str, Any]]],
    target_minutes_expected: Optional[List[int]] = None,
) -> OddsTrajectoryContext:
    """
    {
    "available": bool,
    "event_id": event_id,
    "target_minutes_expected": List[int] ex. [120, 30, 5, 0, -5],
    "target_minutes_present": [...],
    "missing_target_minutes": [...],
    "markets": {
        market_group: {
            market_period: {
                market_name: {
                    choice_group_key: {
                        "market_id": ...,
                        "market_group": ...,
                        "market_period": ...,
                        "market_name": ...,
                        "choice_group": ...,
                        "bookies": {
                            bookie_name: {
                                "bookie_id": ...,
                                "bookie_name": ...,
                                "choices": {
                                    choice_name: {
                                        "choice_id": ...,
                                        "choice_name": ...,
                                        "initial_odds": ...,
                                        "odds_values": {
                                            target_minute: odds_value
                                        },
                                        "meta_by_minute": {
                                            target_minute: {
                                                "snapshot_id": ...,
                                                "collected_at": ...,
                                                "minutes_before_start": ...,
                                                "target_minute": ...,
                                                "distance_from_target": ...
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    expected_minutes = _normalize_expected_minutes(target_minutes_expected)

    if not isinstance(odds_trajectory, list) or not odds_trajectory:
        return OddsTrajectoryContext(
            available=False,
            event_id=None,
            target_minutes_expected=expected_minutes,
            target_minutes_present=[],
            missing_target_minutes=list(expected_minutes),
            markets={},
        )

    markets: Dict[str, Dict[str, Dict[str, Dict[str, MarketLineOddsTrajectory]]]] = {}
    event_id: Optional[int] = None
    present_minutes: set[int] = set()
    available = False

    for row in odds_trajectory:
        if not isinstance(row, dict):
            continue

        market_group = _coerce_text(row.get("market_group"))
        market_period = _coerce_text(row.get("market_period"))
        market_name = _coerce_text(row.get("market_name"))
        choice_name = _coerce_text(row.get("choice_name"))
        target_minute = _coerce_int(row.get("target_minute"))
        odds_value = _coerce_decimal(row.get("odds_value"))

        if (
            market_group is None
            or market_period is None
            or market_name is None
            or choice_name is None
            or target_minute is None
            or odds_value is None
        ):
            continue

        current_event_id = _coerce_int(row.get("event_id"))
        if event_id is None and current_event_id is not None:
            event_id = current_event_id

        choice_group_value = _coerce_text(row.get("choice_group"))
        choice_group_key = _normalize_choice_group_key(choice_group_value)
        bookie_name = _coerce_text(row.get("bookie_name")) or "__unknown__"
        bookie_key = bookie_name

        market_line = _get_market_line_container(
            markets,
            market_group=market_group,
            market_period=market_period,
            market_name=market_name,
            choice_group_key=choice_group_key,
            market_id=_coerce_int(row.get("market_id")),
        )
        bookie = _get_bookie_container(
            market_line,
            bookie_key=bookie_key,
            bookie_id=_coerce_int(row.get("bookie_id")),
            bookie_name=bookie_name,
        )
        choice = _get_choice_container(
            bookie,
            choice_name=choice_name,
            choice_id=_coerce_int(row.get("choice_id")),
            initial_odds=_coerce_decimal(row.get("initial_odds")),
        )

        meta = OddsPointMeta(
            snapshot_id=_coerce_int(row.get("snapshot_id")),
            collected_at=_coerce_datetime(row.get("collected_at")),
            minutes_before_start=_coerce_int(row.get("minutes_before_start")),
            target_minute=target_minute,
            distance_from_target=_coerce_int(row.get("distance_from_target")),
        )

        existing_meta = choice.meta_by_minute.get(target_minute)
        if existing_meta is None or _is_better_candidate(meta, existing_meta):
            choice.odds_values[target_minute] = odds_value
            choice.meta_by_minute[target_minute] = meta
            present_minutes.add(target_minute)
            available = True

    target_minutes_present = [minute for minute in expected_minutes if minute in present_minutes]
    missing_target_minutes = [minute for minute in expected_minutes if minute not in present_minutes]

    return OddsTrajectoryContext(
        available=available,
        event_id=event_id,
        target_minutes_expected=expected_minutes,
        target_minutes_present=target_minutes_present,
        missing_target_minutes=missing_target_minutes,
        markets=markets,
    )


def get_market_line(
    context: OddsTrajectoryContext,
    market_group: str,
    market_period: str,
    market_name: Optional[str] = None,
    choice_group: Optional[str] = None,
) -> Optional[MarketLineOddsTrajectory]:
    market_groups = context.markets.get(market_group)
    if not market_groups:
        return None

    market_periods = market_groups.get(market_period)
    if not market_periods:
        return None

    resolved_market_name = market_name
    if resolved_market_name is None:
        if len(market_periods) != 1:
            return None
        resolved_market_name = next(iter(market_periods))

    market_lines = market_periods.get(resolved_market_name)
    if not market_lines:
        return None

    choice_group_key = _normalize_choice_group_key(choice_group)
    return market_lines.get(choice_group_key)


def get_choice_odds_values(
    market_line: MarketLineOddsTrajectory,
    choice_name: str,
    bookie_name: str = "SofaScore",
) -> Dict[int, Decimal]:
    if market_line is None:
        return {}

    bookie = market_line.bookies.get(bookie_name)
    if bookie is None:
        return {}

    choice = bookie.choices.get(choice_name)
    if choice is None:
        return {}

    return choice.odds_values
