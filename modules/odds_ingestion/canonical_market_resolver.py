"""Data-driven canonical market key resolution for SofaScore and OddsPapi."""

from __future__ import annotations

import re
from typing import Callable

from infrastructure.persistence.catalogs.canonical_market_types import CANONICAL_MARKET_TYPE_SEEDS
from modules.oddspapi.format_utils import normalized_compact, normalized_token
from modules.oddspapi.period_aliases import resolve_canonical_period
from modules.oddspapi.sport_filters import is_allowed_sport_id

_SPACE = re.compile(r"\s+")


def _semantic_token(value) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return _SPACE.sub(" ", text.lower().replace("-", " ")).strip()


def outcome_role(source_outcome_name) -> str | None:
    token = normalized_token(source_outcome_name)
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


def outcome_roles_from_outcomes(outcomes) -> set[str]:
    roles: set[str] = set()
    for outcome in outcomes or []:
        if not isinstance(outcome, dict):
            continue
        role = outcome_role(outcome.get("outcomeName") or outcome.get("name"))
        if role is not None:
            roles.add(role)
    return roles


def _is_side_3way(roles: set[str]) -> bool:
    return roles == {"1", "x", "2"}


def _is_side_2way(roles: set[str]) -> bool:
    return roles == {"1", "2"}


def _is_total(roles: set[str]) -> bool:
    return roles == {"over", "under"}


def _is_decision(roles: set[str]) -> bool:
    return roles == {"yes", "no"}


def _is_side_combo(roles: set[str]) -> bool:
    return roles == {"1x", "x2", "12"}


def _is_goal_team(roles: set[str]) -> bool:
    return "1" in roles and "2" in roles


OUTCOME_VALIDATORS: dict[str, Callable[[set[str]], bool]] = {
    "side_3way": _is_side_3way,
    "side_2way": _is_side_2way,
    "total": _is_total,
    "team_total": _is_total,
    "decision": _is_decision,
    "side_combo": _is_side_combo,
    "goal_team": _is_goal_team,
    "spread_2way": _is_side_2way,
}

FAMILY_CHOICE_ROLES: dict[str, set[str]] = {
    "side_3way": {"1", "x", "2"},
    "side_2way": {"1", "2"},
    "spread_2way": {"1", "2"},
    "total": {"over", "under"},
    "team_total": {"over", "under"},
    "side_combo": {"1x", "x2", "12"},
    "decision": {"yes", "no"},
    "goal_team": {"1", "no_goal", "no", "2"},
}


def _seed_entries() -> list[tuple[str, dict]]:
    return sorted(
        CANONICAL_MARKET_TYPE_SEEDS.items(),
        key=lambda item: (item[1].get("display_order") is None, item[1].get("display_order") or 0, item[0]),
    )


def _sofascore_rule_matches(rule: dict, name: str, group: str, period: str) -> bool:
    if not rule:
        return False
    market_names = {str(value).lower() for value in (rule.get("market_name") or set())}
    market_groups = {str(value).lower() for value in (rule.get("market_group") or set())}
    market_periods = {str(value).lower() for value in (rule.get("market_period") or set())}
    if market_names and name not in market_names:
        return False
    if market_groups and group not in market_groups:
        return False
    if market_periods and period not in market_periods:
        return False
    return bool(market_names or market_groups or market_periods)


def _oddspapi_match_specificity(rule: dict, market_type: str, market_name: str) -> int:
    """Return match specificity for an oddspapi_match rule.

    Scoring:
    - 0: no match
    - 1: exact market_type match (and no market_name constraint, or name not required)
    - 2: exact market_type + exact market_name match

    Exact market_name uses semantic tokens (lowercased, hyphen/space normalized).
    """
    if not rule:
        return 0

    exact_types = {normalized_compact(value) for value in (rule.get("market_type") or set())}
    if not exact_types or not market_type or market_type not in exact_types:
        return 0

    excluded_names = {_semantic_token(value) for value in (rule.get("exclude_market_names") or set())}
    if market_name and market_name in excluded_names:
        return 0

    exact_names = {_semantic_token(value) for value in (rule.get("market_name") or set())}
    if exact_names:
        if market_name and market_name in exact_names:
            return 2
        return 0

    return 1


def _outcomes_valid_for_seed(seed: dict, roles: set[str], rule: dict | None = None) -> bool:
    rule = rule or {}
    if rule.get("skip_outcome_validation"):
        return True

    allowed_sets = rule.get("outcome_role_sets")
    if allowed_sets:
        return any(roles == set(role_set) for role_set in allowed_sets)

    family = seed.get("market_family")
    validator = OUTCOME_VALIDATORS.get(family or "")
    if validator is None:
        return False
    return validator(roles)


def resolve_sofascore_key(market: dict) -> str | None:
    if not isinstance(market, dict):
        return None

    name = _semantic_token(market.get("marketName"))
    group = _semantic_token(market.get("marketGroup"))
    period = _semantic_token(market.get("marketPeriod"))

    candidates: list[tuple[str, dict]] = []
    for canonical_key, seed in _seed_entries():
        rule = seed.get("sofascore_match")
        if not isinstance(rule, dict):
            continue
        if _sofascore_rule_matches(rule, name, group, period):
            candidates.append((canonical_key, seed))

    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0][0]

    roles = outcome_roles_from_outcomes(market.get("choices", []))
    # SofaScore 1st-quarter payloads historically share Home/Away group; prefer family fit.
    validated = [
        (key, seed)
        for key, seed in candidates
        if _outcomes_valid_for_seed(seed, roles)
    ]
    if len(validated) == 1:
        return validated[0][0]
    if not validated:
        # Fall back to draw/length heuristic used by the previous normalizer.
        has_draw = any(
            str(choice.get("name", "")).strip().lower() == "x"
            for choice in market.get("choices", [])
            if isinstance(choice, dict)
        )
        if has_draw or len(market.get("choices", [])) == 3:
            for key, seed in candidates:
                if seed.get("market_family") == "side_3way":
                    return key
        for key, seed in candidates:
            if seed.get("market_family") == "side_2way":
                return key
        return candidates[0][0]
    return validated[0][0]


def resolve_oddspapi_key(item: dict) -> tuple[str | None, str]:
    if not isinstance(item, dict):
        return None, "invalid_catalog_item"
    if bool(item.get("playerProp")):
        return None, "player_prop_unsupported"
    if not is_allowed_sport_id(item.get("sportId")):
        return None, "sport_id_not_allowed"

    canonical_period, period_suffix_or_reason = resolve_canonical_period(item)
    if canonical_period is None:
        return None, period_suffix_or_reason or "unsupported_period"

    market_type = normalized_compact(item.get("marketType"))
    market_name = _semantic_token(item.get("marketName"))
    roles = outcome_roles_from_outcomes(item.get("outcomes", []))

    candidates: list[tuple[int, int, str, dict, dict]] = []
    for canonical_key, seed in _seed_entries():
        rule = seed.get("oddspapi_match")
        if not isinstance(rule, dict):
            continue
        period_lock = rule.get("period_lock")
        if period_lock and period_lock != canonical_period:
            continue
        specificity = _oddspapi_match_specificity(rule, market_type, market_name)
        if specificity <= 0:
            continue
        display_order = seed.get("display_order") or 0
        candidates.append((specificity, display_order, canonical_key, seed, rule))

    if not candidates:
        return None, "unsupported_market_type"

    # Prefer exact market_name matches, then earlier display_order.
    candidates.sort(key=lambda row: (-row[0], row[1], row[2]))

    outcome_failures = 0
    for _specificity, _order, canonical_key, seed, rule in candidates:
        if not _outcomes_valid_for_seed(seed, roles, rule):
            outcome_failures += 1
            continue
        return canonical_key, f"matched_{canonical_key}"

    if outcome_failures:
        return None, "unsupported_market_outcomes"
    return None, "unsupported_market_type"
