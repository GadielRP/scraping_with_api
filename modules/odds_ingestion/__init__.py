from .adapters.oddspapi_market_adapter import OddspapiMarketAdapter
from .adapters.sofascore_market_adapter import SofaScoreMarketAdapter
from .adapters.oddsportal_market_adapter import OddsPortalMarketAdapter, OddsPortalOddsResponse
from .market_odds_ingestion_service import (
    MarketIngestionResult,
    MarketOddsIngestionService,
    OddsPortalIngestionReferenceData,
)
from .canonical_market_normalizer import CanonicalMarketNormalizer, MarketNormalizationContext
from .fetch_result import OddsFetchResult, OddsFetchStatus
from .provider_odds_phase import (
    ProviderOddsSummary,
    is_eligible_for_source,
    mark_missing_endpoints_unavailable,
    restrict_candidates_to_tracked_competitions,
    run_provider_odds_phase,
    select_candidates_for_source,
    should_extract_odds,
)

__all__ = [
    "MarketIngestionResult",
    "MarketOddsIngestionService",
    "OddsPortalIngestionReferenceData",
    "CanonicalMarketNormalizer",
    "MarketNormalizationContext",
    "OddspapiMarketAdapter",
    "SofaScoreMarketAdapter",
    "OddsPortalMarketAdapter",
    "OddsPortalOddsResponse",
    "OddsFetchResult",
    "OddsFetchStatus",
    "ProviderOddsSummary",
    "is_eligible_for_source",
    "mark_missing_endpoints_unavailable",
    "restrict_candidates_to_tracked_competitions",
    "run_provider_odds_phase",
    "select_candidates_for_source",
    "should_extract_odds",
]
