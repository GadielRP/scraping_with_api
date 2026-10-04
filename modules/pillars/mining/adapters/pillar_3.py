"""P3 schema v4 mining writer."""

from .market import MarketMiningAdapter


class P3MiningAdapter(MarketMiningAdapter):
    pillar_id = "pillar_3_totals_market_context"
    result_scope = "totals_market_context"
