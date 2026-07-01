"""Canonical choice normalization, isolated from provider adapters."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ChoiceNormalizationContext:
    market_family: str
    canonical_market_key: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    choice_group: str | None = None


@dataclass(frozen=True)
class ChoiceNormalizationResult:
    resolved: bool
    canonical_choice_name: str | None = None
    choice_group: str | None = None
    reason: str | None = None


class ChoiceNormalizer:
    _PREFIXED_CHOICE = re.compile(r"^\(\s*([-+]?\d+(?:\.\d+)?)\s*\)\s*(.+)$")

    @staticmethod
    def _team_side(value: str, context: ChoiceNormalizationContext) -> str | None:
        token = value.strip().casefold()
        if context.home_team and token == context.home_team.strip().casefold():
            return "1"
        if context.away_team and token == context.away_team.strip().casefold():
            return "2"
        return None

    @staticmethod
    def normalize_choice_name(raw_choice_name: str, context: ChoiceNormalizationContext) -> ChoiceNormalizationResult:
        raw = str(raw_choice_name or "").strip()
        if not raw:
            return ChoiceNormalizationResult(False, reason="missing_choice_name")

        choice_group = context.choice_group
        value = raw
        match = ChoiceNormalizer._PREFIXED_CHOICE.match(raw)
        if match:
            choice_group = match.group(1).strip() or choice_group
            value = match.group(2).strip()

        token = value.casefold()
        family = context.market_family
        canonical = None
        if family == "side_3way":
            canonical = {"1": "1", "x": "x", "draw": "x", "2": "2"}.get(token)
        elif family in {"side_2way", "spread_2way"}:
            canonical = token if token in {"1", "2"} else ChoiceNormalizer._team_side(value, context)
        elif family in {"total", "team_total"}:
            canonical = {"over": "over", "under": "under"}.get(token)
        elif family == "side_combo":
            canonical = {"1x": "1x", "x2": "x2", "12": "12"}.get(token)
        elif family == "decision":
            canonical = {"yes": "yes", "no": "no"}.get(token)
        elif family == "goal_team":
            canonical = {"1": "1", "2": "2", "no goal": "no_goal", "no_goal": "no_goal", "no": "no_goal"}.get(token)
            canonical = canonical or ChoiceNormalizer._team_side(value, context)

        if canonical is None:
            return ChoiceNormalizationResult(False, choice_group=choice_group, reason="unsupported_choice_for_market_family")
        return ChoiceNormalizationResult(True, canonical, choice_group)
