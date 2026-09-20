"""Repository for Pillar 5 exact price memory lookup over mv_p5_price_memory."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


@dataclass(frozen=True, slots=True)
class HistoricalPriceMatch:
    """Historical finished match with exact market odds and verified score."""

    event_id: int
    sport: str
    competition_id: int | None
    bookie_id: int
    market_group: str
    market_period: str
    starts_at: datetime
    odds_home: float
    odds_draw: float | None
    odds_away: float
    home_score: int
    away_score: int
    winner_side: str
    last_sync_at: datetime | None = None


class Pillar5PriceMemoryRepository:
    """Queries mv_p5_price_memory for exact historical odds matches."""

    def __init__(self, session_factory: sessionmaker[Session] | Any) -> None:
        self._session_factory = session_factory

    def find_exact_matches(
        self,
        *,
        bookie_id: int,
        market_group: str,
        market_period: str,
        odds_home: float | Decimal,
        odds_away: float | Decimal,
        odds_draw: float | Decimal | None = None,
        sport: str | None = None,
        competition_id: int | None = None,
        limit: int = 50,
    ) -> list[HistoricalPriceMatch]:
        """Find finished historical events matching exact odds for the given market scope."""
        conditions = [
            "bookie_id = :bookie_id",
            "market_group = :market_group",
            "market_period = :market_period",
            "odds_home = :odds_home",
            "odds_away = :odds_away",
        ]
        params: dict[str, Any] = {
            "bookie_id": bookie_id,
            "market_group": market_group,
            "market_period": market_period,
            "odds_home": float(odds_home),
            "odds_away": float(odds_away),
            "limit": limit,
        }

        if odds_draw is not None:
            conditions.append("odds_draw = :odds_draw")
            params["odds_draw"] = float(odds_draw)
        else:
            conditions.append("odds_draw IS NULL")

        if sport is not None:
            conditions.append("sport = :sport")
            params["sport"] = sport

        if competition_id is not None:
            conditions.append("competition_id = :competition_id")
            params["competition_id"] = competition_id

        where_clause = " AND ".join(conditions)
        sql = text(
            f"""
            SELECT
                event_id,
                sport,
                competition_id,
                bookie_id,
                market_group,
                market_period,
                starts_at,
                odds_home,
                odds_draw,
                odds_away,
                home_score,
                away_score,
                winner_side,
                last_sync_at
            FROM mv_p5_price_memory
            WHERE {where_clause}
            ORDER BY starts_at DESC
            LIMIT :limit
            """
        )

        with self._session_factory() as session:
            rows = session.execute(sql, params).mappings().all()
            return [
                HistoricalPriceMatch(
                    event_id=row["event_id"],
                    sport=row["sport"],
                    competition_id=row["competition_id"],
                    bookie_id=row["bookie_id"],
                    market_group=row["market_group"],
                    market_period=row["market_period"],
                    starts_at=row["starts_at"],
                    odds_home=float(row["odds_home"]),
                    odds_draw=float(row["odds_draw"]) if row["odds_draw"] is not None else None,
                    odds_away=float(row["odds_away"]),
                    home_score=int(row["home_score"]),
                    away_score=int(row["away_score"]),
                    winner_side=row["winner_side"],
                    last_sync_at=row["last_sync_at"],
                )
                for row in rows
            ]
