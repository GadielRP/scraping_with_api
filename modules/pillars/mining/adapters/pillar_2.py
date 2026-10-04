"""P2 schema v4 mining writer."""

from .market import MarketMiningAdapter


class P2MiningAdapter(MarketMiningAdapter):
    pillar_id = "pillar_2_side_market"
    result_scope = "side_market"
