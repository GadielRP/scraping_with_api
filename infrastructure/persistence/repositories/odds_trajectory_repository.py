from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy import bindparam, text

from infrastructure.persistence.database import db_manager
from infrastructure.settings import Config

import logging


logger = logging.getLogger(__name__)


@dataclass
class OddsTrajectoryPoint:
    event_id: int
    market_id: Optional[int]
    market_name: Optional[str]
    market_group: Optional[str]
    market_period: Optional[str]
    choice_group: Optional[str]
    bookie_id: Optional[int]
    bookie_name: Optional[str]
    choice_id: Optional[int]
    choice_name: Optional[str]
    initial_odds: Optional[Decimal]
    odds_value: Optional[Decimal]
    snapshot_id: Optional[int]
    collected_at: Optional[datetime]
    minutes_before_start: Optional[int]
    target_minute: Optional[int]
    distance_from_target: Optional[int]

    def to_dict(self) -> Dict:
        return {
            "event_id": self.event_id,
            "market_id": self.market_id,
            "market_name": self.market_name,
            "market_group": self.market_group,
            "market_period": self.market_period,
            "choice_group": self.choice_group,
            "bookie_id": self.bookie_id,
            "bookie_name": self.bookie_name,
            "choice_id": self.choice_id,
            "choice_name": self.choice_name,
            "initial_odds": self.initial_odds,
            "odds_value": self.odds_value,
            "snapshot_id": self.snapshot_id,
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
            market_name=data.get("market_name"),
            market_group=data.get("market_group"),
            market_period=data.get("market_period"),
            choice_group=data.get("choice_group"),
            bookie_id=data.get("bookie_id"),
            bookie_name=data.get("bookie_name"),
            choice_id=data.get("choice_id"),
            choice_name=data.get("choice_name"),
            initial_odds=data.get("initial_odds"),
            odds_value=data.get("odds_value"),
            snapshot_id=data.get("snapshot_id"),
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
        if not event_ids:
            return {}

        target_minutes = Config.PRE_START_ODDS_MOMENTS if target_minutes is None else target_minutes
        tolerance_minutes = (
            Config.PRE_START_ODDS_MOMENT_TOLERANCE_MINUTES
            if tolerance_minutes is None
            else tolerance_minutes
        )

        markets = Config.MARKETS_DUAL_PROCESS
        periods = Config.PERIODS_DUAL_PROCESS

        if not target_minutes or not markets or not periods:
            return {}

        target_value_rows = ", ".join(
            f"(:target_minute_{idx})" for idx, _ in enumerate(target_minutes)
        )
        target_minute_params = {
            f"target_minute_{idx}": target_minute
            for idx, target_minute in enumerate(target_minutes)
        }

        query = text(
            f"""
            WITH target_moments AS (
                SELECT target_minute
                FROM (VALUES {target_value_rows}) AS tm(target_minute)
            ),
            candidate_rows AS (
                SELECT
                    traj.*,
                    tm.target_minute,
                    ABS(traj.minutes_before_start - tm.target_minute) AS distance_from_target
                FROM v_pre_start_odds_trajectory traj
                CROSS JOIN target_moments tm
                WHERE traj.event_id IN :event_ids
                  AND traj.bookie_id = 1
                  AND (
                        traj.market_name IN :markets
                        OR traj.market_group IN :markets
                  )
                  AND traj.market_period IN :periods
                  AND traj.choice_name IN ('1', 'X', '2')
                  AND ABS(traj.minutes_before_start - tm.target_minute) <= :tolerance_minutes
            ),
            ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            event_id,
                            market_id,
                            bookie_id,
                            choice_id,
                            target_minute
                        ORDER BY
                            distance_from_target ASC,
                            collected_at DESC,
                            snapshot_id DESC
                    ) AS rn
                FROM candidate_rows
            )
            SELECT *
            FROM ranked
            WHERE rn = 1
            ORDER BY
                event_id,
                market_group,
                market_period,
                choice_group NULLS FIRST,
                bookie_name,
                target_minute DESC,
                choice_name;
            """
        ).bindparams(
            bindparam("event_ids", expanding=True),
            bindparam("markets", expanding=True),
            bindparam("periods", expanding=True),
        )

        try:
            with db_manager.get_session() as session:
                rows = session.execute(
                    query,
                    {
                        "event_ids": event_ids,
                        "markets": markets,
                        "periods": periods,
                        "tolerance_minutes": tolerance_minutes,
                        **target_minute_params,
                    },
                ).mappings().all()
        except Exception as exc:
            logger.warning("Failed to load pre-start odds trajectory: %s", exc, exc_info=True)
            return {}

        grouped: Dict[int, List[OddsTrajectoryPoint]] = {}
        for row in rows:
            point = OddsTrajectoryRepository._from_row(row)
            grouped.setdefault(point.event_id, []).append(point)
        return grouped
