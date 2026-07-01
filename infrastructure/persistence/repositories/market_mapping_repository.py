"""Repository helpers for canonical market mappings imported from local catalogs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.orm import Session, joinedload

from infrastructure.persistence.database import db_manager
from infrastructure.persistence.models import (
    CanonicalMarketType,
    MarketOutcomeSourceMapping,
    MarketSourceMapping,
)

logger = logging.getLogger(__name__)


CANONICAL_MARKET_TYPE_SEEDS = {
    # ── Side Markets: 1X2 (3-way) ─────────────────────────────────────
    "1x2_full_time": {
        "canonical_market_name": "1X2 Full Time",
        "canonical_market_group": "1X2",
        "canonical_market_period": "Full Time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 10,
    },
    "1x2_1st_half": {
        "canonical_market_name": "1X2 1st Half",
        "canonical_market_group": "1X2",
        "canonical_market_period": "1st Half",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 11,
    },
    "1x2_1st_quarter": {
        "canonical_market_name": "1X2 1st Quarter",
        "canonical_market_group": "1X2",
        "canonical_market_period": "1st Quarter",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 12,
    },
    # ── Side Markets: Home/Away (2-way) ───────────────────────────────
    "home_away_full_time": {
        "canonical_market_name": "Home/Away Full Time",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "Full Time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 20,
    },
    "home_away_1st_half": {
        "canonical_market_name": "Home/Away 1st Half",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "1st Half",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 21,
    },
    "home_away_1st_quarter": {
        "canonical_market_name": "Home/Away 1st Quarter",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "1st Quarter",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 22,
    },
    "home_away_full_time_including_overtime": {
        "canonical_market_name": "Home/Away Full Time Including Overtime",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "Full Time Including Overtime",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 23,
    },
    # ── Side Markets: Special set/period winners ──────────────────────
    "first_set_winner_1st_set": {
        "canonical_market_name": "First Set Winner 1st Set",
        "canonical_market_group": "First Set Winner",
        "canonical_market_period": "1st Set",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 24,
    },
    "current_set_winner_current_set": {
        "canonical_market_name": "Current Set Winner Current Set",
        "canonical_market_group": "Current Set Winner",
        "canonical_market_period": "Current Set",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 25,
    },
    # ── Totals: Over/Under ────────────────────────────────────────────
    "over_under_full_time": {
        "canonical_market_name": "Over/Under Full Time",
        "canonical_market_group": "Over/Under",
        "canonical_market_period": "Full Time",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 30,
    },
    "over_under_1st_half": {
        "canonical_market_name": "Over/Under 1st Half",
        "canonical_market_group": "Over/Under",
        "canonical_market_period": "1st Half",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 31,
    },
    "over_under_1st_period": {
        "canonical_market_name": "Over/Under 1st Period",
        "canonical_market_group": "Over/Under",
        "canonical_market_period": "1st Period",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 32,
    },
    # ── Totals: Sport-specific ────────────────────────────────────────
    "total_cards_full_time": {
        "canonical_market_name": "Total Cards Full Time",
        "canonical_market_group": "Total Cards",
        "canonical_market_period": "Full Time",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 33,
    },
    "corners_2_way_full_time": {
        "canonical_market_name": "Corners 2-Way Full Time",
        "canonical_market_group": "Corners 2-Way",
        "canonical_market_period": "Full Time",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 34,
    },
    "total_sets_games_extra_time": {
        "canonical_market_name": "Total Sets/Games Extra Time",
        "canonical_market_group": "Total Sets/Games",
        "canonical_market_period": "Extra Time",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 35,
    },
    # ── Team Totals ───────────────────────────────────────────────────
    "team_total_home_full_time": {
        "canonical_market_name": "Team Total Home Full Time",
        "canonical_market_group": "Over/Under Team 1",
        "canonical_market_period": "Full Time",
        "market_family": "team_total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 36,
    },
    "team_total_away_full_time": {
        "canonical_market_name": "Team Total Away Full Time",
        "canonical_market_group": "Over/Under Team 2",
        "canonical_market_period": "Full Time",
        "market_family": "team_total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 37,
    },
    # ── Handicap / Spread ─────────────────────────────────────────────
    "asian_handicap_full_time": {
        "canonical_market_name": "Asian Handicap Full Time",
        "canonical_market_group": "Asian Handicap",
        "canonical_market_period": "Full Time",
        "market_family": "spread",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 40,
    },
    "asian_handicap_1st_half": {
        "canonical_market_name": "Asian Handicap 1st Half",
        "canonical_market_group": "Asian Handicap",
        "canonical_market_period": "1st Half",
        "market_family": "spread",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 41,
    },
    "point_spread_full_time": {
        "canonical_market_name": "Point Spread Full Time",
        "canonical_market_group": "Point Spread",
        "canonical_market_period": "Full Time",
        "market_family": "spread",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 42,
    },
    "european_handicap_full_time": {
        "canonical_market_name": "European Handicap Full Time",
        "canonical_market_group": "European Handicap",
        "canonical_market_period": "Full Time",
        "market_family": "spread_3way",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 43,
    },
    # ── Combination / Draw Markets ────────────────────────────────────
    "draw_no_bet_full_time": {
        "canonical_market_name": "Draw No Bet Full Time",
        "canonical_market_group": "Draw No Bet",
        "canonical_market_period": "Full Time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 50,
    },
    "double_chance_full_time": {
        "canonical_market_name": "Double Chance Full Time",
        "canonical_market_group": "Double Chance",
        "canonical_market_period": "Full Time",
        "market_family": "side_combo",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 51,
    },
    "both_teams_to_score_full_time": {
        "canonical_market_name": "Both Teams To Score Full Time",
        "canonical_market_group": "Both Teams To Score",
        "canonical_market_period": "Full Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 52,
    },
    # ── Goal Specials ─────────────────────────────────────────────────
    "first_goal_full_time": {
        "canonical_market_name": "First Goal Full Time",
        "canonical_market_group": "First Goal",
        "canonical_market_period": "Full Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 60,
    },
    "last_goal_full_time": {
        "canonical_market_name": "Last Goal Full Time",
        "canonical_market_group": "Last Goal",
        "canonical_market_period": "Full Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 61,
    },
    "first_team_to_score_full_time": {
        "canonical_market_name": "First Team To Score Full Time",
        "canonical_market_group": "First Team To Score",
        "canonical_market_period": "Full Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 62,
    },
    "next_goal_full_time": {
        "canonical_market_name": "Next Goal Full Time",
        "canonical_market_group": "Next Goal",
        "canonical_market_period": "Full Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 63,
    },
    # ── Tennis / Volleyball Specials ───────────────────────────────────
    "tie_break_in_match_extra_time": {
        "canonical_market_name": "Tie Break In Match Extra Time",
        "canonical_market_group": "Tie Break In Match",
        "canonical_market_period": "Extra Time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 70,
    },
}


@dataclass
class CanonicalMarketResolution:
    resolved: bool
    mapping_id: int | None = None
    canonical_market_key: str | None = None
    canonical_market_name: str | None = None
    canonical_market_group: str | None = None
    canonical_market_period: str | None = None
    market_family: str | None = None
    requires_choice_group: bool = False
    source_handicap: str | None = None
    reason: str | None = None


@dataclass
class CanonicalOutcomeResolution:
    resolved: bool
    canonical_choice_name: str | None = None
    display_order: int | None = None
    reason: str | None = None


@dataclass
class MarketMappingIndex:
    market_mappings: dict
    outcome_mappings: dict


class MarketMappingRepository:
    FULL_TIME_PERIOD_ALIASES = {"fulltime", "ft", "match", "result", "full-time", "fulltime(includingovertime)"}
    FIRST_QUARTER_PERIOD_ALIASES = {"1stquarter", "firstquarter"}
    FIRST_HALF_PERIOD_ALIASES = {"1sthalf", "firsthalf"}
    FIRST_SET_PERIOD_ALIASES = {"1stset", "firstset"}
    CURRENT_SET_PERIOD_ALIASES = {"currentset"}
    FIRST_PERIOD_PERIOD_ALIASES = {"1stperiod", "firstperiod"}
    EXTRA_TIME_PERIOD_ALIASES = {"extratime", "overtime"}
    SECOND_HALF_PERIOD_ALIASES = {"2ndhalf", "secondhalf"}
    TWO_WAY_MARKET_TYPES = {"moneyline", "homeaway", "matchwinner", "winner"}

    @staticmethod
    def _normalize_source(source) -> str:
        return str(source or "").strip().lower()

    @staticmethod
    def _normalize_source_id(value) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    @staticmethod
    def _normalize_handicap(value) -> str | None:
        return MarketMappingRepository._format_line(value)

    @staticmethod
    def _format_line(value) -> str | None:
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            normalized = str(value).strip()
            return normalized or None
        if number == 0:
            return "0"
        if number.is_integer():
            return str(int(number))
        return str(number).rstrip("0").rstrip(".")

    @staticmethod
    def _normalize_name(value) -> str:
        return str(value or "").strip()

    @staticmethod
    def _normalized_token(value) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _normalized_compact(value) -> str:
        return (
            str(value or "")
            .strip()
            .lower()
            .replace("_", "")
            .replace("-", "")
            .replace(" ", "")
        )

    @staticmethod
    def _canonical_key(base: str, period_suffix: str) -> str:
        return f"{base}_{period_suffix}"

    @staticmethod
    def _resolve_canonical_period(item: dict) -> tuple[str | None, str | None]:
        period_compact = MarketMappingRepository._normalized_compact(item.get("period"))
        market_name_compact = MarketMappingRepository._normalized_compact(item.get("marketName"))

        if period_compact in MarketMappingRepository.FULL_TIME_PERIOD_ALIASES:
            return "Full Time", "full_time"

        if period_compact in MarketMappingRepository.FIRST_QUARTER_PERIOD_ALIASES:
            return "1st Quarter", "1st_quarter"

        if period_compact in MarketMappingRepository.FIRST_HALF_PERIOD_ALIASES:
            return "1st Half", "1st_half"

        if period_compact in MarketMappingRepository.FIRST_SET_PERIOD_ALIASES:
            return "1st Set", "1st_set"

        if period_compact in MarketMappingRepository.CURRENT_SET_PERIOD_ALIASES:
            return "Current Set", "current_set"

        if period_compact in MarketMappingRepository.FIRST_PERIOD_PERIOD_ALIASES:
            return "1st Period", "1st_period"

        if period_compact in MarketMappingRepository.EXTRA_TIME_PERIOD_ALIASES:
            return "Extra Time", "extra_time"

        if period_compact == "p1":
            if "1sthalf" in market_name_compact or "firsthalf" in market_name_compact:
                return "1st Half", "1st_half"
            return None, "unsupported_period_context"

        if period_compact in MarketMappingRepository.SECOND_HALF_PERIOD_ALIASES:
            return None, "unsupported_period"

        if period_compact == "p2":
            if "2ndhalf" in market_name_compact or "secondhalf" in market_name_compact:
                return None, "unsupported_period"
            return None, "unsupported_period_context"

        return None, "unsupported_period"

    @staticmethod
    def _outcome_role(source_outcome_name) -> str | None:
        token = MarketMappingRepository._normalized_token(source_outcome_name)
        return {
            "1": "1",
            "home": "1",
            "x": "x",
            "draw": "x",
            "2": "2",
            "away": "2",
            "over": "over",
            "under": "under",
            "yes": "yes",
            "no": "no",
            "1x": "1x",
            "x2": "x2",
            "12": "12",
            "nogoal": "no_goal",
            "no goal": "no_goal",
        }.get(token)

    @staticmethod
    def _outcome_roles(item: dict) -> set[str]:
        roles = set()
        for outcome in item.get("outcomes", []):
            if not isinstance(outcome, dict):
                continue
            role = MarketMappingRepository._outcome_role(outcome.get("outcomeName"))
            if role is not None:
                roles.add(role)
        return roles

    @staticmethod
    def _is_two_way_side_market(outcome_roles: set[str]) -> bool:
        return outcome_roles == {"1", "2"}

    @staticmethod
    def _is_three_way_side_market(outcome_roles: set[str]) -> bool:
        return outcome_roles == {"1", "x", "2"}

    @staticmethod
    def _is_total_market(outcome_roles: set[str]) -> bool:
        return outcome_roles == {"over", "under"}

    @staticmethod
    def seed_canonical_market_types(session: Optional[Session] = None) -> list[CanonicalMarketType]:
        def _seed(active_session: Session) -> list[CanonicalMarketType]:
            seeded = []
            for canonical_market_key, values in CANONICAL_MARKET_TYPE_SEEDS.items():
                row = active_session.get(CanonicalMarketType, canonical_market_key)
                if row is None:
                    row = CanonicalMarketType(canonical_market_key=canonical_market_key)
                    active_session.add(row)
                for field_name, field_value in values.items():
                    setattr(row, field_name, field_value)
                seeded.append(row)
            active_session.flush()
            return seeded

        if session is not None:
            return _seed(session)
        with db_manager.get_session() as db_session:
            return _seed(db_session)

    @staticmethod
    def resolve_canonical_key_from_catalog_market(item: dict) -> tuple[str | None, str]:
        if not isinstance(item, dict):
            return None, "invalid_catalog_item"
        if bool(item.get("playerProp")):
            return None, "player_prop_unsupported"

        canonical_period, period_suffix_or_reason = MarketMappingRepository._resolve_canonical_period(item)
        if canonical_period is None:
            return None, period_suffix_or_reason or "unsupported_period"

        period_suffix = period_suffix_or_reason
        market_type = MarketMappingRepository._normalized_compact(item.get("marketType"))
        market_name = MarketMappingRepository._normalized_compact(item.get("marketName"))
        outcome_roles = MarketMappingRepository._outcome_roles(item)

        if market_type == "1x2":
            if not MarketMappingRepository._is_three_way_side_market(outcome_roles):
                return None, "unsupported_1x2_outcomes"
            return MarketMappingRepository._canonical_key("1x2", period_suffix), "matched_1x2"

        if market_type == "moneyline":
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_moneyline_outcomes"
            return MarketMappingRepository._canonical_key("home_away", period_suffix), "matched_moneyline"

        if market_type in MarketMappingRepository.TWO_WAY_MARKET_TYPES:
            if MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return MarketMappingRepository._canonical_key("home_away", period_suffix), "matched_two_way_side_market"
            if MarketMappingRepository._is_three_way_side_market(outcome_roles):
                return MarketMappingRepository._canonical_key("1x2", period_suffix), "matched_three_way_side_market"
            return None, "unsupported_side_market_outcomes"

        if market_type == "drawnobet":
            if canonical_period != "Full Time":
                return None, "unsupported_draw_no_bet_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_draw_no_bet_outcomes"
            return "draw_no_bet_full_time", "matched_draw_no_bet"

        if market_type == "doublechance":
            if canonical_period != "Full Time":
                return None, "unsupported_double_chance_period"
            if outcome_roles != {"1x", "x2", "12"}:
                return None, "unsupported_double_chance_outcomes"
            return "double_chance_full_time", "matched_double_chance"

        if market_type == "bothteamsscore":
            if canonical_period != "Full Time":
                return None, "unsupported_btts_period"
            if outcome_roles != {"yes", "no"}:
                return None, "unsupported_btts_outcomes"
            return "both_teams_to_score_full_time", "matched_btts"

        if market_type == "firstgoal" or "firstgoal" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_first_goal_period"
            if outcome_roles == {"1", "no_goal", "2"}:
                return "first_goal_full_time", "matched_first_goal"
            return None, "unsupported_first_goal_outcomes"

        if market_type in {"lastteamtoscore", "lastgoal"} or "lastteamtoscore" in market_name or "lastgoal" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_last_goal_period"
            if outcome_roles == {"1", "no_goal", "2"}:
                return "last_goal_full_time", "matched_last_goal"
            return None, "unsupported_last_goal_outcomes"

        if market_type == "1stquarterwinner" or "1stquarterwinner" in market_name:
            if canonical_period != "1st Quarter":
                return None, "unsupported_1st_quarter_period"
            if not MarketMappingRepository._is_three_way_side_market(outcome_roles):
                return None, "unsupported_1st_quarter_outcomes"
            return "1x2_1st_quarter", "matched_1x2_1st_quarter"

        if market_type == "fulltime(includingovertime)" or "fulltime(includingovertime)" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_home_away_including_overtime_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_home_away_including_overtime_outcomes"
            return "home_away_full_time_including_overtime", "matched_home_away_including_overtime"

        if market_type == "firstsetwinner" or "firstsetwinner" in market_name:
            if canonical_period != "1st Set":
                return None, "unsupported_first_set_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_first_set_outcomes"
            return "first_set_winner_1st_set", "matched_first_set_winner"

        if market_type == "currentsetwinner" or "currentsetwinner" in market_name:
            if canonical_period != "Current Set":
                return None, "unsupported_current_set_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_current_set_outcomes"
            return "current_set_winner_current_set", "matched_current_set_winner"

        if market_type == "pointspread" or "pointspread" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_point_spread_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_point_spread_outcomes"
            return "point_spread_full_time", "matched_point_spread"

        if market_type == "gametotal" or "gametotal" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_game_total_outcomes"
            return "over_under_full_time", "matched_game_total"

        if market_type == "totalpoints" or "totalpoints" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_total_points_outcomes"
            return "over_under_full_time", "matched_total_points"

        if market_type == "matchgoals" or "matchgoals" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_match_goals_outcomes"
            return "over_under_full_time", "matched_match_goals"

        if market_type == "1stperiodgoals" or "1stperiodgoals" in market_name:
            if canonical_period != "1st Period":
                return None, "unsupported_1st_period_goals_period"
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_1st_period_goals_outcomes"
            return "over_under_1st_period", "matched_1st_period_goals"

        if market_type == "totalcards" or "cardsinmatch" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_cards_in_match_outcomes"
            return "total_cards_full_time", "matched_cards_in_match"

        if market_type == "corners2way" or "corners2way" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_corners_2_way_outcomes"
            return "corners_2_way_full_time", "matched_corners_2_way"

        if market_type == "totalsets/games" or "totalgameswon" in market_name:
            if canonical_period != "Extra Time":
                return None, "unsupported_total_games_won_period"
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_total_games_won_outcomes"
            return "total_sets_games_extra_time", "matched_total_games_won"

        if market_type == "firstteamtoscore" or "firstteamtoscore" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_first_team_to_score_period"
            return "first_team_to_score_full_time", "matched_first_team_to_score"

        if market_type == "nextgoal" or "nextgoal" in market_name:
            if canonical_period != "Full Time":
                return None, "unsupported_next_goal_period"
            return "next_goal_full_time", "matched_next_goal"

        if market_type == "tiebreakinmatch" or "tiebreakinmatch" in market_name:
            if canonical_period != "Extra Time":
                return None, "unsupported_tie_break_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles) and outcome_roles != {"yes", "no"}:
                return None, "unsupported_tie_break_outcomes"
            return "tie_break_in_match_extra_time", "matched_tie_break_in_match"

        if "total" in market_type or "overunder" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_total_outcomes"
            if canonical_period == "1st Half":
                return "over_under_1st_half", "matched_over_under_1st_half"
            if market_type == "teamtotalsteam1":
                return "team_total_home_full_time", "matched_team_total_home"
            if market_type == "teamtotalsteam2":
                return "team_total_away_full_time", "matched_team_total_away"
            return "over_under_full_time", "matched_over_under_full_time"

        if "spread" in market_type or "handicap" in market_type or "handicap" in market_name:
            if MarketMappingRepository._is_three_way_side_market(outcome_roles):
                if canonical_period != "Full Time":
                    return None, "unsupported_european_handicap_period"
                return "european_handicap_full_time", "matched_european_handicap"
            if MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return MarketMappingRepository._canonical_key(
                    "asian_handicap",
                    period_suffix,
                ), "matched_asian_handicap"
            return None, "unsupported_spread_outcomes"

        return None, "unsupported_market_type"

    @staticmethod
    def canonical_choice_from_outcome(
        canonical_market_key: str,
        source_outcome_name,
    ) -> str | None:
        role = MarketMappingRepository._outcome_role(source_outcome_name)
        if canonical_market_key in {"1x2_full_time", "1x2_1st_half", "1x2_1st_quarter", "european_handicap_full_time"}:
            return role if role in {"1", "x", "2"} else None
        if canonical_market_key in {
            "home_away_full_time",
            "home_away_1st_half",
            "home_away_1st_quarter",
            "home_away_full_time_including_overtime",
            "first_set_winner_1st_set",
            "current_set_winner_current_set",
            "asian_handicap_full_time",
            "asian_handicap_1st_half",
            "point_spread_full_time",
            "draw_no_bet_full_time",
        }:
            return role if role in {"1", "2"} else None
        if canonical_market_key in {
            "over_under_full_time",
            "over_under_1st_half",
            "over_under_1st_period",
            "total_cards_full_time",
            "corners_2_way_full_time",
            "team_total_home_full_time",
            "team_total_away_full_time",
            "total_sets_games_extra_time",
        }:
            return role if role in {"over", "under"} else None
        if canonical_market_key == "double_chance_full_time":
            return role if role in {"1x", "x2", "12"} else None
        if canonical_market_key == "both_teams_to_score_full_time":
            return role if role in {"yes", "no"} else None
        if canonical_market_key in {
            "first_goal_full_time",
            "last_goal_full_time",
            "first_team_to_score_full_time",
        }:
            return role if role in {"1", "no_goal", "no", "2"} else None
        if canonical_market_key == "next_goal_full_time":
            return role if role in {"1", "no", "no_goal", "2"} else None
        if canonical_market_key == "tie_break_in_match_extra_time":
            return role if role in {"yes", "no"} else None
        return None

    @staticmethod
    def upsert_market_source_mapping_from_catalog_item(
        item,
        source: str = "oddspapi",
        session: Optional[Session] = None,
        include_unsupported: bool = False,
    ) -> MarketSourceMapping | None:
        normalized_source = MarketMappingRepository._normalize_source(source)
        if not normalized_source:
            raise ValueError("source is required")
        if not isinstance(item, dict):
            raise ValueError("catalog item must be a dict")

        source_sport_id = MarketMappingRepository._normalize_source_id(item.get("sportId"))
        source_market_id = MarketMappingRepository._normalize_source_id(item.get("marketId"))
        source_market_name = MarketMappingRepository._normalize_name(item.get("marketName"))
        source_market_group = MarketMappingRepository._normalize_name(item.get("marketType")) or None
        source_period = MarketMappingRepository._normalize_name(item.get("period")) or None
        source_handicap = MarketMappingRepository._normalize_handicap(item.get("handicap"))
        player_prop = bool(item.get("playerProp"))

        if not source_market_id:
            raise ValueError("source_market_id is required")
        if not source_market_name:
            raise ValueError("source_market_name is required")

        canonical_market_key, _ = MarketMappingRepository.resolve_canonical_key_from_catalog_market(item)
        if canonical_market_key is None:
            if include_unsupported:
                logger.info(
                    "Skipping unsupported market mapping import for source=%s market_id=%s; unsupported rows are not persisted in phase 2.0",
                    normalized_source,
                    source_market_id,
                )
            return None

        def _upsert(active_session: Session) -> MarketSourceMapping | None:
            canonical_market_type = active_session.get(CanonicalMarketType, canonical_market_key)
            if canonical_market_type is None:
                MarketMappingRepository.seed_canonical_market_types(active_session)
                canonical_market_type = active_session.get(CanonicalMarketType, canonical_market_key)
            if canonical_market_type is None:
                raise ValueError(f"Missing canonical market seed: {canonical_market_key}")

            query = active_session.query(MarketSourceMapping).filter(
                MarketSourceMapping.source == normalized_source,
                MarketSourceMapping.source_market_id == source_market_id,
            )
            if source_sport_id is None:
                query = query.filter(MarketSourceMapping.source_sport_id.is_(None))
            else:
                query = query.filter(MarketSourceMapping.source_sport_id == source_sport_id)

            mapping = query.first()
            if mapping is None:
                mapping = MarketSourceMapping(
                    source=normalized_source,
                    source_sport_id=source_sport_id,
                    source_market_id=source_market_id,
                )
                active_session.add(mapping)

            mapping.canonical_market_key = canonical_market_key
            mapping.source_market_name = source_market_name
            mapping.source_market_group = source_market_group
            mapping.source_period = source_period
            mapping.source_handicap = source_handicap
            mapping.player_prop = player_prop
            mapping.canonical_market_name = canonical_market_type.canonical_market_name
            mapping.canonical_market_group = canonical_market_type.canonical_market_group
            mapping.canonical_market_period = canonical_market_type.canonical_market_period
            mapping.match_method = "catalog_rule"
            mapping.confidence = 1.000
            active_session.flush()

            existing_outcomes = {
                outcome.source_outcome_id: outcome
                for outcome in (
                    active_session.query(MarketOutcomeSourceMapping)
                    .filter(
                        MarketOutcomeSourceMapping.market_source_mapping_id == mapping.mapping_id
                    )
                    .all()
                )
            }
            seen_outcome_ids = set()
            for display_order, outcome in enumerate(item.get("outcomes", []), start=1):
                if not isinstance(outcome, dict):
                    continue
                source_outcome_id = MarketMappingRepository._normalize_source_id(
                    outcome.get("outcomeId")
                )
                source_outcome_name = MarketMappingRepository._normalize_name(
                    outcome.get("outcomeName")
                )
                if not source_outcome_id or not source_outcome_name:
                    continue
                canonical_choice_name = MarketMappingRepository.canonical_choice_from_outcome(
                    canonical_market_key,
                    source_outcome_name,
                )
                if canonical_choice_name is None:
                    continue

                seen_outcome_ids.add(source_outcome_id)
                outcome_mapping = existing_outcomes.get(source_outcome_id)
                if outcome_mapping is None:
                    outcome_mapping = MarketOutcomeSourceMapping(
                        market_source_mapping_id=mapping.mapping_id,
                        source_outcome_id=source_outcome_id,
                    )
                    active_session.add(outcome_mapping)
                outcome_mapping.source_outcome_name = source_outcome_name
                outcome_mapping.canonical_choice_name = canonical_choice_name
                outcome_mapping.display_order = display_order

            for source_outcome_id, stale_mapping in existing_outcomes.items():
                if source_outcome_id not in seen_outcome_ids:
                    active_session.delete(stale_mapping)

            active_session.flush()
            return mapping

        if session is not None:
            return _upsert(session)
        with db_manager.get_session() as db_session:
            return _upsert(db_session)

    @staticmethod
    def build_index(
        source: str = "oddspapi",
        enabled_only: bool = True,
    ) -> MarketMappingIndex:
        normalized_source = MarketMappingRepository._normalize_source(source)
        if not normalized_source:
            return MarketMappingIndex(market_mappings={}, outcome_mappings={})

        with db_manager.get_session() as session:
            query = (
                session.query(MarketSourceMapping)
                .options(
                    joinedload(MarketSourceMapping.canonical_market_type),
                    joinedload(MarketSourceMapping.outcome_mappings),
                )
                .filter(MarketSourceMapping.source == normalized_source)
            )
            market_mappings = {}
            outcome_mappings = {}
            for mapping in query.all():
                canonical_market_type = mapping.canonical_market_type
                if canonical_market_type is None:
                    continue
                if enabled_only and not canonical_market_type.enabled_for_ingestion:
                    continue

                market_key = (
                    normalized_source,
                    MarketMappingRepository._normalize_source_id(mapping.source_sport_id),
                    MarketMappingRepository._normalize_source_id(mapping.source_market_id),
                )
                market_mappings[market_key] = CanonicalMarketResolution(
                    resolved=True,
                    mapping_id=mapping.mapping_id,
                    canonical_market_key=mapping.canonical_market_key,
                    canonical_market_name=mapping.canonical_market_name,
                    canonical_market_group=mapping.canonical_market_group,
                    canonical_market_period=mapping.canonical_market_period,
                    market_family=canonical_market_type.market_family,
                    requires_choice_group=bool(canonical_market_type.requires_choice_group),
                    source_handicap=mapping.source_handicap,
                    reason="resolved_from_db_mapping",
                )

                for outcome_mapping in mapping.outcome_mappings:
                    outcome_key = (
                        mapping.mapping_id,
                        MarketMappingRepository._normalize_source_id(
                            outcome_mapping.source_outcome_id
                        ),
                    )
                    outcome_mappings[outcome_key] = CanonicalOutcomeResolution(
                        resolved=True,
                        canonical_choice_name=outcome_mapping.canonical_choice_name,
                        display_order=outcome_mapping.display_order,
                        reason="resolved_from_db_mapping",
                    )

            return MarketMappingIndex(
                market_mappings=market_mappings,
                outcome_mappings=outcome_mappings,
            )

    @staticmethod
    def resolve_market(
        index: MarketMappingIndex,
        source: str,
        source_sport_id,
        source_market_id,
    ) -> CanonicalMarketResolution:
        if index is None:
            return CanonicalMarketResolution(
                resolved=False,
                reason="market_mapping_index_unavailable",
            )

        normalized_source = MarketMappingRepository._normalize_source(source)
        normalized_sport_id = MarketMappingRepository._normalize_source_id(source_sport_id)
        normalized_market_id = MarketMappingRepository._normalize_source_id(source_market_id)
        if not normalized_source or not normalized_market_id:
            return CanonicalMarketResolution(
                resolved=False,
                reason="invalid_market_lookup_key",
            )

        exact_key = (normalized_source, normalized_sport_id, normalized_market_id)
        resolved = index.market_mappings.get(exact_key)
        if resolved is not None:
            return resolved

        fallback_key = (normalized_source, None, normalized_market_id)
        resolved = index.market_mappings.get(fallback_key)
        if resolved is not None:
            return resolved

        return CanonicalMarketResolution(resolved=False, reason="market_mapping_not_found")

    @staticmethod
    def resolve_outcome(
        index: MarketMappingIndex,
        market_source_mapping_id: int | None,
        source_outcome_id,
    ) -> CanonicalOutcomeResolution:
        if index is None:
            return CanonicalOutcomeResolution(
                resolved=False,
                reason="market_mapping_index_unavailable",
            )
        normalized_outcome_id = MarketMappingRepository._normalize_source_id(source_outcome_id)
        if market_source_mapping_id is None or not normalized_outcome_id:
            return CanonicalOutcomeResolution(
                resolved=False,
                reason="invalid_outcome_lookup_key",
            )

        resolved = index.outcome_mappings.get((market_source_mapping_id, normalized_outcome_id))
        if resolved is not None:
            return resolved
        return CanonicalOutcomeResolution(resolved=False, reason="outcome_mapping_not_found")
