import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from time import perf_counter
from typing import Dict, List, Optional

from sqlalchemy import text

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.repositories.odds_trajectory_query import (
    build_pre_start_trajectory_query,
)
from infrastructure.settings import Config

logger = logging.getLogger(__name__)


class OddsTrajectoryLoadError(RuntimeError):
    """Raised when the trajectory read fails before producing a valid result."""


@dataclass
class OddsTrajectoryPoint:
    event_id: int
    market_id: Optional[int]
    canonical_market_key: Optional[str]
    market_family: Optional[str]
    market_display_order: Optional[int]
    market_name: Optional[str]
    market_group: Optional[str]
    market_period: Optional[str]
    choice_group: Optional[str]
    bookie_id: Optional[int]
    bookie_name: Optional[str]
    choice_id: Optional[int]
    choice_name: Optional[str]
    choice_display_order: Optional[int]
    quote_id: Optional[int]
    source: Optional[str]
    exchange_side: Optional[str]
    exchange_level: Optional[int]
    initial_odds: Optional[Decimal]
    odds_value: Optional[Decimal]
    snapshot_id: Optional[int]
    source_collected_at: Optional[datetime]
    collected_at: Optional[datetime]
    minutes_before_start: Optional[int]
    target_minute: Optional[int]
    distance_from_target: Optional[int]

    def to_dict(self) -> Dict:
        return {
            "event_id": self.event_id,
            "market_id": self.market_id,
            "canonical_market_key": self.canonical_market_key,
            "market_family": self.market_family,
            "market_display_order": self.market_display_order,
            "market_name": self.market_name,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "choice_group": self.choice_group,
            "bookie_id": self.bookie_id,
            "bookie_name": self.bookie_name,
            "choice_id": self.choice_id,
            "choice_name": self.choice_name,
            "choice_display_order": self.choice_display_order,
            "quote_id": self.quote_id,
            "source": self.source,
            "exchange_side": self.exchange_side,
            "exchange_level": self.exchange_level,
            "initial_odds": self.initial_odds,
            "odds_value": self.odds_value,
            "snapshot_id": self.snapshot_id,
            "source_collected_at": self.source_collected_at,
            "collected_at": self.collected_at,
            "minutes_before_start": self.minutes_before_start,
            "target_minute": self.target_minute,
            "distance_from_target": self.distance_from_target,
        }


class OddsTrajectoryRepository:
    @staticmethod
    def _from_row(row) -> OddsTrajectoryPoint:
        data = dict(row)
        return OddsTrajectoryPoint(
            event_id=data["event_id"],
            market_id=data.get("market_id"),
            canonical_market_key=data.get("canonical_market_key"),
            market_family=data.get("market_family"),
            market_display_order=data.get("market_display_order"),
            market_name=data.get("market_name"),
            market_group=data.get("market_group"),
            market_period=data.get("market_period"),
            choice_group=data.get("choice_group"),
            bookie_id=data.get("bookie_id"),
            bookie_name=data.get("bookie_name"),
            choice_id=data.get("choice_id"),
            choice_name=data.get("choice_name"),
            choice_display_order=data.get("choice_display_order"),
            quote_id=data.get("quote_id"),
            source=data.get("source"),
            exchange_side=data.get("exchange_side"),
            exchange_level=data.get("exchange_level"),
            initial_odds=data.get("initial_odds"),
            odds_value=data.get("odds_value"),
            snapshot_id=data.get("snapshot_id"),
            source_collected_at=data.get("source_collected_at"),
            collected_at=data.get("collected_at"),
            minutes_before_start=data.get("minutes_before_start"),
            target_minute=data.get("target_minute"),
            distance_from_target=data.get("distance_from_target"),
        )

    @staticmethod
    def get_pre_start_trajectory_map(
        event_ids: List[int],
        target_minutes: Optional[List[int]] = None,
        tolerance_minutes: Optional[int] = None,
    ) -> Dict[int, List[OddsTrajectoryPoint]]:
        normalized_event_ids = sorted({int(event_id) for event_id in event_ids})
        if not normalized_event_ids:
            return {}

        target_minutes = Config.PRE_START_ODDS_MOMENTS if target_minutes is None else target_minutes
        normalized_target_minutes = list(
            dict.fromkeys(int(target_minute) for target_minute in target_minutes)
        )
        tolerance_minutes = (
            Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES
            if tolerance_minutes is None
            else tolerance_minutes
        )

        if not normalized_target_minutes:
            return {}
        if tolerance_minutes < 0:
            raise ValueError("tolerance_minutes must be non-negative")

        return OddsTrajectoryRepository._load_pre_start_trajectory_map(
            event_ids=normalized_event_ids,
            target_minutes=normalized_target_minutes,
            tolerance_minutes=int(tolerance_minutes),
        )

    @staticmethod
    def _load_pre_start_trajectory_map(
        *,
        event_ids: List[int],
        target_minutes: List[int],
        tolerance_minutes: int,
    ) -> Dict[int, List[OddsTrajectoryPoint]]:
        target_minute_params = {
            f"target_minute_{idx}": target_minute
            for idx, target_minute in enumerate(target_minutes)
        }
        query_params = {
            "event_ids": event_ids,
            "tolerance_minutes": tolerance_minutes,
            **target_minute_params,
        }
        query = build_pre_start_trajectory_query(target_minutes)
        started_at = perf_counter()

        try:
            with db_manager.get_session() as session:
                get_bind = getattr(session, "get_bind", None)
                bind = get_bind() if callable(get_bind) else None
                timeout_ms = Config.PRE_START_ODDS_TRAJECTORY_QUERY_TIMEOUT_MS
                if (
                    timeout_ms > 0
                    and bind is not None
                    and bind.dialect.name == "postgresql"
                ):
                    session.execute(
                        text(
                            "SELECT set_config("
                            "'statement_timeout', :timeout_value, true)"
                        ),
                        {"timeout_value": f"{timeout_ms}ms"},
                    )
                rows = session.execute(
                    query,
                    query_params,
                ).mappings().all()

            grouped: Dict[int, List[OddsTrajectoryPoint]] = {}
            for row in rows:
                point = OddsTrajectoryRepository._from_row(row)
                grouped.setdefault(point.event_id, []).append(point)
        except Exception as exc:
            duration_ms = (perf_counter() - started_at) * 1000
            logger.exception(
                "Failed to load event-scoped pre-start odds trajectory "
                "events=%s targets=%s tolerance=%s duration_ms=%.1f",
                len(event_ids),
                len(target_minutes),
                tolerance_minutes,
                duration_ms,
            )
            raise OddsTrajectoryLoadError(
                "Failed to load event-scoped pre-start odds trajectory"
            ) from exc

        duration_ms = (perf_counter() - started_at) * 1000
        logger.info(
            "Loaded event-scoped pre-start odds trajectory "
            "events_requested=%s events_returned=%s targets=%s rows=%s "
            "duration_ms=%.1f",
            len(event_ids),
            len(grouped),
            len(target_minutes),
            len(rows),
            duration_ms,
        )
        return grouped


__all__ = [
    "OddsTrajectoryLoadError",
    "OddsTrajectoryPoint",
    "OddsTrajectoryRepository",
]
