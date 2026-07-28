"""Optional canonical enrichment for the regular-odds comparator.

Builds a lookup dict from a pre-existing MarketMappingIndex without
coupling the pure comparator to the database or repository layer.
"""

from __future__ import annotations

from modules.oddspapi.diagnostics.regular_odds_comparator import EnrichmentInfo
from modules.oddspapi.format_utils import normalize_source_id


def build_enrichment_from_index(
    index,
    *,
    source: str = "oddspapi",
    source_sport_id: str | int | None = None,
) -> dict[tuple[str, str], EnrichmentInfo]:
    """Build an enrichment lookup from a MarketMappingIndex.

    Returns a dict keyed by ``(source_market_id, source_outcome_id)``
    mapping to :class:`EnrichmentInfo`.

    The ``index`` is expected to be a :class:`MarketMappingIndex` from
    ``infrastructure.persistence.repositories.market_mapping_repository``.
    This function accesses only its ``market_mappings`` and ``outcome_mappings``
    dicts — it does not import or instantiate any repository.
    """
    result: dict[tuple[str, str], EnrichmentInfo] = {}

    if index is None:
        return result

    market_mappings = getattr(index, "market_mappings", None) or {}
    outcome_mappings = getattr(index, "outcome_mappings", None) or {}

    norm_source = str(source or "").strip().lower()
    norm_sport = normalize_source_id(source_sport_id)

    for market_key, resolution in market_mappings.items():
        if not isinstance(market_key, tuple) or len(market_key) < 3:
            continue
        mk_source, mk_sport, mk_market_id = market_key
        if mk_source != norm_source:
            continue
        if norm_sport is not None and mk_sport is not None and mk_sport != norm_sport:
            continue

        mapping_id = getattr(resolution, "mapping_id", None)

        for outcome_key, outcome_resolution in outcome_mappings.items():
            if not isinstance(outcome_key, tuple) or len(outcome_key) < 2:
                continue
            ok_mapping_id, ok_outcome_id = outcome_key
            if ok_mapping_id != mapping_id:
                continue

            lookup_key = (str(mk_market_id), str(ok_outcome_id))
            if lookup_key in result:
                continue

            result[lookup_key] = EnrichmentInfo(
                source_sport_id=mk_sport,
                canonical_market_key=getattr(resolution, "canonical_market_key", None),
                canonical_market_name=getattr(resolution, "canonical_market_name", None),
                canonical_market_group=getattr(resolution, "canonical_market_group", None),
                canonical_market_period=getattr(resolution, "canonical_market_period", None),
                market_family=getattr(resolution, "market_family", None),
                canonical_choice_name=getattr(outcome_resolution, "canonical_choice_name", None),
                handicap=getattr(resolution, "source_handicap", None),
                choice_group=getattr(resolution, "source_handicap", None),
            )

    return result
