from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy import bindparam, text

from infrastructure.persistence.database import db_manager


@dataclass
class DualProcessOdds:
    event_id: int
    market_id: Optional[int]
    market_name: Optional[str]
    market_group: Optional[str]
    market_period: Optional[str]
    bookie_id: Optional[int]
    one_open: Optional[Decimal]
    one_final: Optional[Decimal]
    x_open: Optional[Decimal]
    x_final: Optional[Decimal]
    two_open: Optional[Decimal]
    two_final: Optional[Decimal]
    var_one: Optional[Decimal]
    var_x: Optional[Decimal]
    var_two: Optional[Decimal]
    var_shape: bool
    last_sync_at: Optional[datetime]


class DualProcessOddsRepository:
    @staticmethod
    def _from_row(row) -> DualProcessOdds:
        data = dict(row)
        return DualProcessOdds(
            event_id=data["event_id"],
            market_id=data.get("market_id"),
            market_name=data.get("market_name"),
            market_group=data.get("market_group"),
            market_period=data.get("market_period"),
            bookie_id=data.get("bookie_id"),
            one_open=data.get("one_open"),
            one_final=data.get("one_final"),
            x_open=data.get("x_open"),
            x_final=data.get("x_final"),
            two_open=data.get("two_open"),
            two_final=data.get("two_final"),
            var_one=data.get("var_one"),
            var_x=data.get("var_x"),
            var_two=data.get("var_two"),
            var_shape=bool(data.get("var_shape")),
            last_sync_at=data.get("last_sync_at"),
        )

    @staticmethod
    def get_event_odds(event_id: int) -> Optional[DualProcessOdds]:
        query = text("SELECT * FROM v_dual_process_event_odds WHERE event_id = :event_id LIMIT 1")
        with db_manager.get_session() as session:
            row = session.execute(query, {"event_id": event_id}).mappings().first()
            return DualProcessOddsRepository._from_row(row) if row else None

    @staticmethod
    def get_event_odds_map(event_ids: List[int]) -> Dict[int, DualProcessOdds]:
        if not event_ids:
            return {}

        query = text("SELECT * FROM v_dual_process_event_odds WHERE event_id IN :event_ids").bindparams(
            bindparam("event_ids", expanding=True)
        )
        with db_manager.get_session() as session:
            rows = session.execute(query, {"event_ids": event_ids}).mappings().all()
            odds = [DualProcessOddsRepository._from_row(row) for row in rows]
            return {item.event_id: item for item in odds}

    @staticmethod
    def event_has_dual_process_odds(event_id: int) -> bool:
        query = text("SELECT 1 FROM v_dual_process_event_odds WHERE event_id = :event_id LIMIT 1")
        with db_manager.get_session() as session:
            return session.execute(query, {"event_id": event_id}).first() is not None
