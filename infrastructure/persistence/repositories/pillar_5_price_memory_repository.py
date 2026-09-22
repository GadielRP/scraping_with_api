"""Repository for Pillar 5 exact price memory lookup over mv_p5_price_memory."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any

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
    odds_home: Decimal
    odds_draw: Decimal | None
    odds_away: Decimal
    home_score: int
    away_score: int
    winner_side: str
    season_id: int | None = None
    country: str | None = None
    last_sync_at: datetime | None = None


def _to_3dp_decimal(val: float | Decimal) -> Decimal:
    """Validate and normalize an odds value exactly like PostgreSQL numeric(8,3)."""
    if val is None or isinstance(val, bool):
        raise ValueError("odds value is required")
    try:
        result = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("odds value must be decimal") from exc
    if not result.is_finite() or result <= Decimal("1"):
        raise ValueError("odds value must be finite and greater than 1")
    return result.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


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
        has_draw: bool,
        sport: str,
        current_event_id: int,
        current_starts_at: datetime,
        odds_draw: float | Decimal | None = None,
        competition_id: int | None = None,
        season_id: int | None = None,
        country: str | None = None,
        limit: int | None = None,
    ) -> list[HistoricalPriceMatch]:
        """Find finished historical events matching exact odds for the given market scope."""
        normalized_sport = str(sport or "").strip()
        if not normalized_sport:
            raise ValueError("sport is required for canonical P5 memory lookup")
        if isinstance(current_event_id, bool) or int(current_event_id) <= 0:
            raise ValueError("current_event_id must be a positive integer")
        if current_starts_at is None:
            raise ValueError("current_starts_at is required for causal lookup")
        if limit is not None and (isinstance(limit, bool) or int(limit) <= 0):
            raise ValueError("limit must be a positive integer or None")
        if has_draw and odds_draw is None:
            raise ValueError("THREE_WAY lookup requires odds_draw")
        if not has_draw and odds_draw is not None:
            raise ValueError("TWO_WAY lookup cannot include odds_draw")

        conditions = [
            "bookie_id = :bookie_id",
            "market_group = :market_group",
            "market_period = :market_period",
            "has_draw = :has_draw",
            "sport = :sport",
            "odds_home = :odds_home",
            "odds_away = :odds_away",
            "event_id != :current_event_id",
            "starts_at < :current_starts_at",
        ]
        params: dict[str, Any] = {
            "bookie_id": bookie_id,
            "market_group": market_group,
            "market_period": market_period,
            "has_draw": has_draw,
            "sport": normalized_sport,
            "odds_home": _to_3dp_decimal(odds_home),
            "odds_away": _to_3dp_decimal(odds_away),
            "current_event_id": int(current_event_id),
            "current_starts_at": current_starts_at,
        }

        if odds_draw is not None:
            conditions.append("odds_draw = :odds_draw")
            params["odds_draw"] = _to_3dp_decimal(odds_draw)
        else:
            conditions.append("odds_draw IS NULL")

        if competition_id is not None:
            conditions.append("competition_id = :competition_id")
            params["competition_id"] = competition_id

        if season_id is not None:
            conditions.append("season_id = :season_id")
            params["season_id"] = season_id

        if country is not None:
            conditions.append("country = :country")
            params["country"] = country

        limit_clause = ""
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
            ORDER BY starts_at DESC, event_id DESC
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
                    odds_home=_to_3dp_decimal(row["odds_home"]),
                    odds_draw=(
                        _to_3dp_decimal(row["odds_draw"])
                        if row["odds_draw"] is not None
                        else None
                    ),
                    odds_away=_to_3dp_decimal(row["odds_away"]),
                    home_score=int(row["home_score"]),
                    away_score=int(row["away_score"]),
                    winner_side=str(row["winner_side"]),
                    last_sync_at=row["last_sync_at"],
                )
                for row in rows
            ]
