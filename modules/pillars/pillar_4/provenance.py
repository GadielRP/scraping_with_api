"""Resolve temporal signal contracts through their actual constituent series."""

from decimal import Decimal, InvalidOperation
from typing import Iterable

from .signal_models import P4SeriesResult


def resolve_series_contracts(
    series: Iterable[P4SeriesResult], contracts: dict,
) -> dict[str, tuple[str, ...]]:
    index = {item.series_id: item for item in series}
    resolved: dict[str, tuple[str, ...]] = {}

    def resolve(key):
        if key in resolved:
            return resolved[key]
        item = index[key]
        constituents = item.traceability.get("CONSTITUENT_SERIES_IDS", ())
        if constituents:
            refs = {ref for child in constituents for ref in resolve(child)}
        else:
            market = item.market
            refs = set()
            for ref, contract in contracts.items():
                if (
                    contract["market_group"] != market["MARKET_GROUP"]
                    or contract["market_period"] != market["MARKET_PERIOD"]
                    or market["BOOKIE_ID"] not in contract["bookie_ids"]
                ):
                    continue
                if market["VALUE_TYPE"] == "LINE":
                    try:
                        used = any(point.value == Decimal(str(contract["line_value"]))
                                   for point in item.points)
                    except InvalidOperation:
                        used = False
                else:
                    used = contract["line_value"] == market["CHOICE_GROUP"]
                if used:
                    refs.add(ref)
        resolved[key] = tuple(sorted(refs))
        return resolved[key]

    for key in index:
        resolve(key)
    return resolved
