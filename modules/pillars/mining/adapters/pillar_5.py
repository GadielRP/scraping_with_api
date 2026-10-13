"""P5 schema v5 mining writer."""

from .market import MarketMiningAdapter


class P5MiningAdapter(MarketMiningAdapter):
    pillar_id = "pillar_5"
    result_scope = "price_memory"
