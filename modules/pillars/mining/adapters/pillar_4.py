"""P4 schema v4 mining writer."""

from .market import MarketMiningAdapter


class P4MiningAdapter(MarketMiningAdapter):
    pillar_id = "pillar_4_temporal_market_drift"
    result_scope = "temporal_market_drift"
