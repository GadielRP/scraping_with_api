"""Historical samples for Pillar 5 exact price memory."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional

from infrastructure.persistence.database import db_manager
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Debug logging helpers
# ---------------------------------------------------------------------------

def _debug_section(title: str) -> None:
    logger.info("========== P5_EXACT_PRICE_MEMORY_ENGINE DEBUG | %s ==========", title)


def _debug_line(message: str, *args: Any) -> None:
    logger.info("P5_EXACT_PRICE_MEMORY_ENGINE DEBUG | " + message, *args)


def _fmt(value: Any, decimals: int = 3) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        preview = ", ".join(f"{k}: {_fmt(v, decimals)}" for k, v in items)
        return f"{{{preview}}} (n={len(items)})"
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        preview = ", ".join(_fmt(item, decimals) for item in sequence)
        return f"[{preview}] (n={len(sequence)})"
    return str(value)


def _format_datetime(dt: Any) -> str:
    if dt is None:
        return "N/A"
    if hasattr(dt, "strftime"):
        return dt.strftime("%Y-%m-%d")
    return str(dt)


@dataclass(frozen=True)
class ExactPriceMemorySample:
    sample_size: int
    wins_home: int
    wins_draw: int
    wins_away: int
    rows: list[dict] = field(default_factory=list)
    historical_matches: list[dict] = field(default_factory=list)


def get_exact_price_memory_sample(
    event_id: int,
    sport: str,
    current_home_odds: Decimal,
    current_away_odds: Decimal,
    current_draw_odds: Optional[Decimal] = None,
    debug_mode: bool = False,
) -> ExactPriceMemorySample:
    """Return historical matches and counts for the exact current odds set and sport."""
    if debug_mode:
        _debug_section("Consulta de Muestras Históricas en BD")
        _debug_line("Parámetros de consulta:")
        _debug_line("  - event_id: %s", event_id)
        _debug_line("  - sport: %s", sport)
        _debug_line("  - current_home_odds: %s", current_home_odds)
        _debug_line("  - current_away_odds: %s", current_away_odds)
        _debug_line("  - current_draw_odds: %s", current_draw_odds)

    try:
        if current_draw_odds is not None:
            query = text(
                """
                SELECT mae.event_id, mae.sport, mae.home_team, mae.away_team, mae.start_time_utc,
                       mae.one_final, mae.x_final, mae.two_final, mae.var_shape, mae.winner_side,
                       mae.home_score, mae.away_score
                FROM mv_alert_events mae
                WHERE mae.event_id != :event_id
                  AND mae.sport = :sport
                  AND ROUND(mae.one_final::numeric, 3) = ROUND(CAST(:current_home_odds AS numeric), 3)
                  AND ROUND(mae.x_final::numeric, 3) = ROUND(CAST(:current_draw_odds AS numeric), 3)
                  AND ROUND(mae.two_final::numeric, 3) = ROUND(CAST(:current_away_odds AS numeric), 3)
                  AND mae.var_shape = true
                  AND mae.winner_side IS NOT NULL
                ORDER BY mae.start_time_utc DESC
                """
            )
            params = {
                "event_id": event_id,
                "sport": sport,
                "current_home_odds": current_home_odds,
                "current_draw_odds": current_draw_odds,
                "current_away_odds": current_away_odds,
            }
        else:
            query = text(
                """
                SELECT mae.event_id, mae.sport, mae.home_team, mae.away_team, mae.start_time_utc,
                       mae.one_final, mae.two_final, mae.var_shape, mae.winner_side,
                       mae.home_score, mae.away_score
                FROM mv_alert_events mae
                WHERE mae.event_id != :event_id
                  AND mae.sport = :sport
                  AND ROUND(mae.one_final::numeric, 3) = ROUND(CAST(:current_home_odds AS numeric), 3)
                  AND ROUND(mae.two_final::numeric, 3) = ROUND(CAST(:current_away_odds AS numeric), 3)
                  AND mae.var_shape = false
                  AND mae.winner_side IS NOT NULL
                ORDER BY mae.start_time_utc DESC
                """
            )
            params = {
                "event_id": event_id,
                "sport": sport,
                "current_home_odds": current_home_odds,
                "current_away_odds": current_away_odds,
            }

        if debug_mode:
            _debug_line("SQL Query:")
            _debug_line(str(query.text))
            _debug_line("Parámetros del query: %s", _fmt(params))

        with db_manager.get_session() as session:
            if debug_mode:
                _debug_line("Ejecutando consulta SQL en base de datos...")
            result_rows = session.execute(query, params).fetchall()

        if debug_mode:
            raw_mapped = [dict(r._mapping) for r in result_rows]
            _debug_line("Resultados crudos de BD: %s", _fmt(raw_mapped))

        historical_matches = []
        wins_home = 0
        wins_draw = 0
        wins_away = 0

        for row in result_rows:
            mapping = row._mapping
            winner_side = mapping.get("winner_side")
            if winner_side == "1":
                wins_home += 1
            elif winner_side == "X":
                wins_draw += 1
            elif winner_side == "2":
                wins_away += 1

            one_final = mapping.get("one_final")
            x_final = mapping.get("x_final") if "x_final" in mapping else None
            two_final = mapping.get("two_final")

            home_score = mapping.get("home_score")
            away_score = mapping.get("away_score")

            match_dict = {
                "event_id": mapping.get("event_id"),
                "sport": mapping.get("sport"),
                "home_team": mapping.get("home_team"),
                "away_team": mapping.get("away_team"),
                "home_score": int(home_score) if home_score is not None else None,
                "away_score": int(away_score) if away_score is not None else None,
                "start_time": _format_datetime(mapping.get("start_time_utc")),
                "one_final": float(one_final) if one_final is not None else None,
                "x_final": float(x_final) if x_final is not None else None,
                "two_final": float(two_final) if two_final is not None else None,
                "var_shape": bool(mapping.get("var_shape")),
                "winner_side": winner_side,
            }
            historical_matches.append(match_dict)

        sample_size = len(historical_matches)

        rows = [
            {"winner_side": "1", "wins_count": wins_home},
            {"winner_side": "2", "wins_count": wins_away},
        ]
        if current_draw_odds is not None:
            rows.append({"winner_side": "X", "wins_count": wins_draw})
        rows.sort(key=lambda item: str(item.get("winner_side") or ""))

        if debug_mode:
            _debug_line("Resultados mapeados y ordenados: %s", _fmt(rows))
            _debug_line("Resultados del conteo:")
            _debug_line("  - wins_home: %d", wins_home)
            _debug_line("  - wins_away: %d", wins_away)
            _debug_line("  - wins_draw: %d", wins_draw)
            _debug_line("  - sample_size: %d", sample_size)
            _debug_line("------------------------------------------------------------")

        return ExactPriceMemorySample(
            sample_size=sample_size,
            wins_home=wins_home,
            wins_draw=wins_draw,
            wins_away=wins_away,
            rows=rows,
            historical_matches=historical_matches,
        )
    except Exception:
        logger.warning("Exact price memory sample query failed for event_id=%s", event_id, exc_info=True)
        return ExactPriceMemorySample(0, 0, 0, 0, rows=[], historical_matches=[])

