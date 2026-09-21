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
    has_draw: bool
    starts_at: datetime
    odds_home: float
    odds_draw: float | None
    odds_away: float
    home_score: int
    away_score: int
    winner_side: str
    season_id: int | None = None
    country: str | None = None
    last_sync_at: datetime | None = None


def _to_3dp_decimal(val: float | Decimal) -> Decimal:
    """Normalize odds value to Decimal with 3 decimal places."""
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.001"))
    return Decimal(str(val)).quantize(Decimal("0.001"))


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
        has_draw: bool | None = None,
        sport: str | None = None,
        competition_id: int | None = None,
        season_id: int | None = None,
        country: str | None = None,
        current_event_id: int | None = None,
        current_starts_at: datetime | None = None,
        limit: int | None = None,
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
            "odds_home": _to_3dp_decimal(odds_home),
            "odds_away": _to_3dp_decimal(odds_away),
        }

        if odds_draw is not None:
            conditions.append("odds_draw = :odds_draw")
            params["odds_draw"] = _to_3dp_decimal(odds_draw)
        else:
            conditions.append("odds_draw IS NULL")

        if has_draw is not None:
            conditions.append("has_draw = :has_draw")
            params["has_draw"] = has_draw

        if sport is not None:
            conditions.append("sport = :sport")
            params["sport"] = sport

        if competition_id is not None:
            conditions.append("competition_id = :competition_id")
            params["competition_id"] = competition_id

        if season_id is not None:
            conditions.append("season_id = :season_id")
            params["season_id"] = season_id

        if country is not None:
            conditions.append("country = :country")
            params["country"] = country

        if current_event_id is not None:
            conditions.append("event_id != :current_event_id")
            params["current_event_id"] = current_event_id

        if current_starts_at is not None:
            conditions.append("starts_at < :current_starts_at")
            params["current_starts_at"] = current_starts_at

        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        if limit is not None:
            params["limit"] = limit
            limit_clause = "LIMIT :limit"

        where_clause = " AND ".join(conditions)
        sql = text(
            f"""
            SELECT
                event_id,
                sport,
                competition_id,
                season_id,
                country,
                bookie_id,
                market_group,
                market_period,
                has_draw,
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
            {limit_clause}
            """
        )

        with self._session_factory() as session:
            rows = session.execute(sql, params).mappings().all()
            return [
                HistoricalPriceMatch(
                    event_id=row["event_id"],
                    sport=row["sport"],
                    competition_id=row["competition_id"],
                    season_id=row["season_id"],
                    country=row["country"],
                    bookie_id=row["bookie_id"],
                    market_group=row["market_group"],
                    market_period=row["market_period"],
                    has_draw=bool(row["has_draw"]),
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
