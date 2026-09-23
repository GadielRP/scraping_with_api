"""Explicit registry for available persisted-data backfill strategies."""

from __future__ import annotations

from typing import Any, Callable

from infrastructure.persistence.backfill.binary_choice_structure_backfill import (
    BinaryChoiceStructureBackfillService,
)
from infrastructure.persistence.backfill.canonical_period_backfill import (
    PERIOD_VARIANT_PAIRS,
    BackfillScope,
    CanonicalPeriodBackfillService,
)
from infrastructure.persistence.backfill.configured_non_draw_sports_backfill import (
    ConfiguredNonDrawSportsBackfillService,
)
from infrastructure.persistence.backfill.strategy import BackfillStrategy

StrategyFactory = Callable[[BackfillScope, dict[str, Any] | None], BackfillStrategy]


def _canonical_period_factory(
    scope: BackfillScope,
    parameters: dict[str, Any] | None = None,
) -> CanonicalPeriodBackfillService:
    raw_pairs = (parameters or {}).get("period_pairs") or PERIOD_VARIANT_PAIRS
    period_pairs = {int(key): int(value) for key, value in raw_pairs.items()}
    return CanonicalPeriodBackfillService(scope=scope, period_pairs=period_pairs)


def _configured_non_draw_sports_factory(
    scope: BackfillScope,
    parameters: dict[str, Any] | None = None,
) -> ConfiguredNonDrawSportsBackfillService:
    raw_pairs = (parameters or {}).get("period_pairs") or PERIOD_VARIANT_PAIRS
    period_pairs = {int(key): int(value) for key, value in raw_pairs.items()}
    return ConfiguredNonDrawSportsBackfillService(
        scope=scope,
        period_pairs=period_pairs,
    )


def _binary_choice_structure_factory(
    scope: BackfillScope,
    parameters: dict[str, Any] | None = None,
) -> BinaryChoiceStructureBackfillService:
    raw_pairs = (parameters or {}).get("period_pairs") or PERIOD_VARIANT_PAIRS
    period_pairs = {int(key): int(value) for key, value in raw_pairs.items()}
    require_binary = bool((parameters or {}).get("require_binary_home_away", True))
    if not require_binary:
        raise ValueError(
            "binary-choice manifests without a Home/Away anchor guard are unsafe; "
            "rerun audit to create a guarded manifest"
        )
    return BinaryChoiceStructureBackfillService(
        scope=scope,
        period_pairs=period_pairs,
        require_binary_home_away=require_binary,
    )


STRATEGY_FACTORIES: dict[str, StrategyFactory] = {
    "canonical_period_backfill_by_fixed_competition_ids_v1": _canonical_period_factory,
    "canonical_period_backfill_by_configured_non_draw_sports_v1": (
        _configured_non_draw_sports_factory
    ),
    "canonical_period_backfill_by_binary_choice_structure_v1": (
        _binary_choice_structure_factory
    ),
}



def create_strategy(
    strategy_name: str,
    *,
    scope: BackfillScope,
    parameters: dict[str, Any] | None = None,
):
    try:
        factory = STRATEGY_FACTORIES[strategy_name]
    except KeyError as exc:
        available = ", ".join(sorted(STRATEGY_FACTORIES))
        raise ValueError(
            f"unknown backfill strategy {strategy_name!r}; available: {available}"
        ) from exc
    return factory(scope, parameters)
