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
    observed_minutes_before_start: Optional[int]
    trajectory_minutes_before_start: Optional[Decimal]
    main_line: Optional[bool] = None
    source_limit: Optional[Decimal] = None
    exchange_size: Optional[Decimal] = None

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
            "main_line": self.main_line,
            "choice_display_order": self.choice_display_order,
            "quote_id": self.quote_id,
            "source": self.source,
            "exchange_side": self.exchange_side,
            "exchange_level": self.exchange_level,
            "initial_odds": self.initial_odds,
            "odds_value": self.odds_value,
            "exchange_size": self.exchange_size,
            "snapshot_id": self.snapshot_id,
            "source_collected_at": self.source_collected_at,
            "collected_at": self.collected_at,
            "observed_minutes_before_start": self.observed_minutes_before_start,
            "trajectory_minutes_before_start": self.trajectory_minutes_before_start,
            "source_limit": self.source_limit,
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
            main_line=data.get("main_line"),
            choice_display_order=data.get("choice_display_order"),
            quote_id=data.get("quote_id"),
            source=data.get("source"),
            exchange_side=data.get("exchange_side"),
            exchange_level=data.get("exchange_level"),
            initial_odds=data.get("initial_odds"),
            odds_value=data.get("odds_value"),
            source_limit=data.get("source_limit"),
            exchange_size=data.get("exchange_size"),
            snapshot_id=data.get("snapshot_id"),
            source_collected_at=data.get("source_collected_at"),
            collected_at=data.get("collected_at"),
            observed_minutes_before_start=data.get("observed_minutes_before_start"),
            trajectory_minutes_before_start=data.get(
                "trajectory_minutes_before_start"
            ),
        )

    @staticmethod
    def get_pre_start_trajectory_map(
        event_ids: List[int],
    ) -> Dict[int, List[OddsTrajectoryPoint]]:
        normalized_event_ids = sorted({int(event_id) for event_id in event_ids})
        if not normalized_event_ids:
            return {}

        return OddsTrajectoryRepository._load_pre_start_trajectory_map(
            event_ids=normalized_event_ids,
        )

    @staticmethod
    def _load_pre_start_trajectory_map(
        *,
        event_ids: List[int],
    ) -> Dict[int, List[OddsTrajectoryPoint]]:
        query_params = {"event_ids": event_ids}
        query = build_pre_start_trajectory_query().execution_options(
            stream_results=True,
            yield_per=1000,
        )
        started_at = perf_counter()
        grouped: Dict[int, List[OddsTrajectoryPoint]] = {}
        row_count = 0

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
                ).mappings()
                for row in rows:
                    point = OddsTrajectoryRepository._from_row(row)
                    grouped.setdefault(point.event_id, []).append(point)
                    row_count += 1
        except Exception as exc:
            duration_ms = (perf_counter() - started_at) * 1000
            logger.exception(
                "Failed to load event-scoped pre-start odds trajectory "
                "events=%s duration_ms=%.1f",
                len(event_ids),
                duration_ms,
            )
            raise OddsTrajectoryLoadError(
                "Failed to load event-scoped pre-start odds trajectory"
            ) from exc

        duration_ms = (perf_counter() - started_at) * 1000
        logger.info(
            "Loaded event-scoped pre-start odds trajectory "
            "events_requested=%s events_returned=%s rows=%s "
            "duration_ms=%.1f",
            len(event_ids),
            len(grouped),
            row_count,
            duration_ms,
        )
        return grouped


__all__ = [
    "OddsTrajectoryLoadError",
    "OddsTrajectoryPoint",
    "OddsTrajectoryRepository",
]
