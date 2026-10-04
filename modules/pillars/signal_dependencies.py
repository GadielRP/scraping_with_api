"""Declared dependencies for the existing P2/P3 analytical profile fields.

Profile paths are internal domain identifiers. Provider names never participate
in dependency matching. Extending a bookmaker registry does not extend these
analytical capabilities automatically.
"""

from dataclasses import dataclass
from types import MappingProxyType

from .market_evaluation import PINNACLE, BET365, BETFAIR, FT, FT_OT

REGULAR = (PINNACLE.id, BET365.id)
EXCHANGE = (BETFAIR.id,)


@dataclass(frozen=True, slots=True)
class Dependencies:
    book_ids: tuple[int, ...]
    families: tuple[str, ...]
    periods: tuple[str, ...]
    sides: tuple[str, ...] = ()

    def accepts(self, trace: dict) -> bool:
        return (
            trace.get("bookie_id") in self.book_ids
            and trace.get("market_group") in self.families
            and trace.get("market_period") in self.periods
            and (not self.sides or trace.get("exchange_side") in self.sides)
        )


# (bookmakers, family selector, temporal selector).
# MONEYLINE resolves to the actual contract, including an incomplete 1X2.
PROFILE_SCOPES = MappingProxyType(
    {
        "EXCHANGE": (EXCHANGE, "MONEYLINE", "FT"),
        "BOOK_EXCHANGE": ((*REGULAR, *EXCHANGE), "MONEYLINE", "FT"),
        "BETFAIR_FT_AH": (EXCHANGE, "Asian Handicap", "FT"),
        "BETFAIR_1H_AH": (EXCHANGE, "Asian Handicap", "SECONDARY"),
        "BOOK_EXCHANGE_AH": ((*REGULAR, *EXCHANGE), "Asian Handicap", "FT"),
        "BOOK_EXCHANGE_1H_AH": ((*REGULAR, *EXCHANGE), "Asian Handicap", "SECONDARY"),
        "BETFAIR_FT_HANDICAP": (EXCHANGE, "Handicap", "FT"),
        "BETFAIR_1H_HANDICAP": (EXCHANGE, "Handicap", "SECONDARY"),
        "BOOK_EXCHANGE_HANDICAP": ((*REGULAR, *EXCHANGE), "Handicap", "FT"),
        "BOOK_EXCHANGE_1H_HANDICAP": ((*REGULAR, *EXCHANGE), "Handicap", "SECONDARY"),
        "BETFAIR_FT_OU": (EXCHANGE, "Over/Under", "FT"),
        "BETFAIR_1H_OU": (EXCHANGE, "Over/Under", "SECONDARY"),
        "BOOK_EXCHANGE_OU": ((*REGULAR, *EXCHANGE), "Over/Under", "FT"),
        "BOOK_EXCHANGE_1H_OU": ((*REGULAR, *EXCHANGE), "Over/Under", "SECONDARY"),
    }
)
PERIOD_FAMILIES = MappingProxyType(
    {
        "1X2": ("MONEYLINE",),
        "AH": ("Asian Handicap",),
        "HANDICAP": ("Handicap",),
        "CROSS_MARKET": ("MONEYLINE", "Asian Handicap"),
        "CROSS_MARKET_HANDICAP": ("MONEYLINE", "Handicap"),
        "PINNACLE": ("Over/Under",),
        "BET365": ("Over/Under",),
        "LINE_STRUCTURE": ("Over/Under",),
        "BOOK_RELATION": ("Over/Under",),
        "REPRESENTATIVE": ("Over/Under",),
    }
)
BOOK_FIELDS = MappingProxyType({"PIN_EDGE": (PINNACLE.id,), "B365_EDGE": (BET365.id,)})
SIDE_FIELDS = MappingProxyType({"BACK_EDGE": ("back",), "LAY_EDGE": ("lay",)})


def resolve_dependencies(
    path: tuple[str, ...], *, moneyline: str | None, secondary: str
) -> Dependencies:
    block = path[0]
    if block in ("FT", "1H"):
        families = PERIOD_FAMILIES.get(path[1], ()) if len(path) > 1 else ()
        books, temporal = REGULAR, "FT" if block == "FT" else "SECONDARY"
        if len(path) > 1 and path[1] in ("PINNACLE", "BET365"):
            books = (PINNACLE.id,) if path[1] == "PINNACLE" else (BET365.id,)
    elif block == "FT_1H":
        books, families, temporal = (
            REGULAR,
            ("MONEYLINE" if moneyline else "Over/Under",),
            "BOTH",
        )
    else:
        books, family, temporal = PROFILE_SCOPES[block]
        families = (family,)
    books = BOOK_FIELDS.get(path[-1], books)
    sides = SIDE_FIELDS.get(path[-1], ())
    if len(path) > 1 and path[1] in ("BACK", "LAY"):
        sides = (path[1].lower(),)
    families = tuple(
        moneyline if family == "MONEYLINE" else family for family in families
    )
    periods = (
        (FT, FT_OT)
        if temporal == "FT"
        else (secondary,) if temporal == "SECONDARY" else (FT, FT_OT, secondary)
    )
    return Dependencies(books, families, periods, sides)
