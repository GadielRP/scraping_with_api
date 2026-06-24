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
    "1x2_full_time": {
        "canonical_market_name": "Full-time",
        "canonical_market_group": "1X2",
        "canonical_market_period": "Full-time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 10,
    },
    "moneyline_full_time": {
        "canonical_market_name": "Full time",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "Full-time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 11,
    },
    "moneyline_1st_half": {
        "canonical_market_name": "1st half",
        "canonical_market_group": "Home/Away",
        "canonical_market_period": "1st Half",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 12,
    },
    "1x2_1st_half": {
        "canonical_market_name": "1st half",
        "canonical_market_group": "1X2",
        "canonical_market_period": "1st Half",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 13,
    },
    "total_full_time": {
        "canonical_market_name": "Total",
        "canonical_market_group": "Over/Under",
        "canonical_market_period": "Full-time",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 20,
    },
    "total_1st_half": {
        "canonical_market_name": "Total",
        "canonical_market_group": "Over/Under",
        "canonical_market_period": "1st Half",
        "market_family": "total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 21,
    },
    "asian_handicap_full_time": {
        "canonical_market_name": "Asian handicap",
        "canonical_market_group": "Asian handicap",
        "canonical_market_period": "Full-time",
        "market_family": "spread",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 30,
    },
    "asian_handicap_1st_half": {
        "canonical_market_name": "Asian handicap",
        "canonical_market_group": "Asian handicap",
        "canonical_market_period": "1st Half",
        "market_family": "spread",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 31,
    },
    "draw_no_bet_full_time": {
        "canonical_market_name": "Draw no bet",
        "canonical_market_group": "Draw no bet",
        "canonical_market_period": "Full-time",
        "market_family": "side",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 40,
    },
    "double_chance_full_time": {
        "canonical_market_name": "Double chance",
        "canonical_market_group": "Double chance",
        "canonical_market_period": "Full-time",
        "market_family": "side_combo",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 41,
    },
    "both_teams_to_score_full_time": {
        "canonical_market_name": "Both teams to score",
        "canonical_market_group": "Both teams to score",
        "canonical_market_period": "Full-time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 42,
    },
    "european_handicap_full_time": {
        "canonical_market_name": "European handicap",
        "canonical_market_group": "European handicap",
        "canonical_market_period": "Full-time",
        "market_family": "spread_3way",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 43,
    },
    "team_total_home_full_time": {
        "canonical_market_name": "Team 1 total",
        "canonical_market_group": "Over/Under Team 1",
        "canonical_market_period": "Full-time",
        "market_family": "team_total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 50,
    },
    "team_total_away_full_time": {
        "canonical_market_name": "Team 2 total",
        "canonical_market_group": "Over/Under Team 2",
        "canonical_market_period": "Full-time",
        "market_family": "team_total",
        "requires_choice_group": True,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": True,
        "display_order": 51,
    },
    "first_goal_full_time": {
        "canonical_market_name": "First goal",
        "canonical_market_group": "First goal",
        "canonical_market_period": "Full-time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 60,
    },
    "last_goal_full_time": {
        "canonical_market_name": "Last goal",
        "canonical_market_group": "Last goal",
        "canonical_market_period": "Full-time",
        "market_family": "special",
        "requires_choice_group": False,
        "enabled_for_ingestion": True,
        "enabled_for_trajectory": False,
        "display_order": 61,
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
    FULL_TIME_PERIOD_ALIASES = {"fulltime", "ft", "match", "result", "full-time"}
    FIRST_HALF_PERIOD_ALIASES = {"1sthalf", "firsthalf"}
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
            return "Full-time", "full_time"

        if period_compact in MarketMappingRepository.FIRST_HALF_PERIOD_ALIASES:
            return "1st Half", "1st_half"

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
            "x": "X",
            "draw": "X",
            "2": "2",
            "away": "2",
            "over": "Over",
            "under": "Under",
            "yes": "Yes",
            "no": "No",
            "1x": "1X",
            "x2": "X2",
            "12": "12",
            "nogoal": "No Goal",
            "no goal": "No Goal",
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
        return outcome_roles == {"1", "X", "2"}

    @staticmethod
    def _is_total_market(outcome_roles: set[str]) -> bool:
        return outcome_roles == {"Over", "Under"}

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
            return MarketMappingRepository._canonical_key("moneyline", period_suffix), "matched_moneyline"

        if market_type in MarketMappingRepository.TWO_WAY_MARKET_TYPES:
            if MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return MarketMappingRepository._canonical_key("moneyline", period_suffix), "matched_two_way_side_market"
            if MarketMappingRepository._is_three_way_side_market(outcome_roles):
                return MarketMappingRepository._canonical_key("1x2", period_suffix), "matched_three_way_side_market"
            return None, "unsupported_side_market_outcomes"

        if market_type == "drawnobet":
            if canonical_period != "Full-time":
                return None, "unsupported_draw_no_bet_period"
            if not MarketMappingRepository._is_two_way_side_market(outcome_roles):
                return None, "unsupported_draw_no_bet_outcomes"
            return "draw_no_bet_full_time", "matched_draw_no_bet"

        if market_type == "doublechance":
            if canonical_period != "Full-time":
                return None, "unsupported_double_chance_period"
            if outcome_roles != {"1X", "X2", "12"}:
                return None, "unsupported_double_chance_outcomes"
            return "double_chance_full_time", "matched_double_chance"

        if market_type == "bothteamsscore":
            if canonical_period != "Full-time":
                return None, "unsupported_btts_period"
            if outcome_roles != {"Yes", "No"}:
                return None, "unsupported_btts_outcomes"
            return "both_teams_to_score_full_time", "matched_btts"

        if market_type in {"firstteamtoscore", "firstgoal"} or "firstteamtoscore" in market_name or "firstgoal" in market_name:
            if canonical_period != "Full-time":
                return None, "unsupported_first_goal_period"
            if outcome_roles == {"1", "No Goal", "2"}:
                return "first_goal_full_time", "matched_first_goal"
            return None, "unsupported_first_goal_outcomes"

        if market_type in {"lastteamtoscore", "lastgoal"} or "lastteamtoscore" in market_name or "lastgoal" in market_name:
            if canonical_period != "Full-time":
                return None, "unsupported_last_goal_period"
            if outcome_roles == {"1", "No Goal", "2"}:
                return "last_goal_full_time", "matched_last_goal"
            return None, "unsupported_last_goal_outcomes"

        if "total" in market_type or "overunder" in market_name:
            if not MarketMappingRepository._is_total_market(outcome_roles):
                return None, "unsupported_total_outcomes"
            if canonical_period == "1st Half":
                return "total_1st_half", "matched_total_1st_half"
            if market_type == "teamtotalsteam1":
                return "team_total_home_full_time", "matched_team_total_home"
            if market_type == "teamtotalsteam2":
                return "team_total_away_full_time", "matched_team_total_away"
            return "total_full_time", "matched_total_full_time"

        if "spread" in market_type or "handicap" in market_type or "handicap" in market_name:
            if MarketMappingRepository._is_three_way_side_market(outcome_roles):
                if canonical_period != "Full-time":
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
        if canonical_market_key in {"1x2_full_time", "1x2_1st_half", "european_handicap_full_time"}:
            return role if role in {"1", "X", "2"} else None
        if canonical_market_key in {
            "moneyline_full_time",
            "moneyline_1st_half",
            "asian_handicap_full_time",
            "asian_handicap_1st_half",
            "draw_no_bet_full_time",
        }:
            return role if role in {"1", "2"} else None
        if canonical_market_key in {
            "total_full_time",
            "total_1st_half",
            "team_total_home_full_time",
            "team_total_away_full_time",
        }:
            return role if role in {"Over", "Under"} else None
        if canonical_market_key == "double_chance_full_time":
            return role if role in {"1X", "X2", "12"} else None
        if canonical_market_key == "both_teams_to_score_full_time":
            return role if role in {"Yes", "No"} else None
        if canonical_market_key in {"first_goal_full_time", "last_goal_full_time"}:
            return role if role in {"1", "No Goal", "2"} else None
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
