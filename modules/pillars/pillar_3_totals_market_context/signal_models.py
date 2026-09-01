"""Immutable DTOs for the canonical Pillar 3 signal profile."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from .relations import ContextDirection, Direction, Relation


def _number(value: Decimal | None) -> float | None:
    return None if value is None else float(value)


@dataclass(frozen=True, slots=True)
class BookOUReading:
    line: Decimal | None
    over_odds: Decimal | None
    under_odds: Decimal | None
    edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "LINE": _number(self.line),
            "OVER_ODDS": _number(self.over_odds),
            "UNDER_ODDS": _number(self.under_odds),
            "EDGE": _number(self.edge),
            "DIRECTION": self.direction,
        }


@dataclass(frozen=True, slots=True)
class LineStructureSignal:
    line_diff_raw: Decimal | None
    line_gap: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "LINE_DIFF_RAW": _number(self.line_diff_raw),
            "LINE_GAP": _number(self.line_gap),
        }


@dataclass(frozen=True, slots=True)
class BookRelationSignal:
    relation: Relation | None
    gap: Decimal | None

    def to_dict(self) -> dict[str, Any]:
        return {"RELATION": self.relation, "GAP": _number(self.gap)}


@dataclass(frozen=True, slots=True)
class RepresentativeSignal:
    edge: Decimal | None
    direction: Direction | None

    def to_dict(self) -> dict[str, Any]:
        return {"EDGE": _number(self.edge), "DIRECTION": self.direction}


@dataclass(frozen=True, slots=True)
class PeriodOUSignal:
    pinnacle: BookOUReading
    bet365: BookOUReading
    line_structure: LineStructureSignal
    book_relation: BookRelationSignal
    representative: RepresentativeSignal
    context_direction_raw: ContextDirection | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "PINNACLE": self.pinnacle.to_dict(),
            "BET365": self.bet365.to_dict(),
            "LINE_STRUCTURE": self.line_structure.to_dict(),
            "BOOK_RELATION": self.book_relation.to_dict(),
            "REPRESENTATIVE": self.representative.to_dict(),
            "CONTEXT_DIRECTION_RAW": self.context_direction_raw,
        }


@dataclass(frozen=True, slots=True)
class FT1HSignal:
    relation: Relation
    gap: Decimal
    ft_book_relation: Relation
    ft_rep_direction: Direction
    first_half_book_relation: Relation
    first_half_rep_direction: Direction

    def to_dict(self) -> dict[str, Any]:
        return {
            "FT_1H_OU_RELATION": self.relation,
            "FT_1H_OU_GAP": _number(self.gap),
            "FT_1H_OU_STRUCTURE": {
                "FT_BOOK_RELATION": self.ft_book_relation,
                "FT_REP_DIRECTION": self.ft_rep_direction,
                "FIRST_HALF_BOOK_RELATION": self.first_half_book_relation,
                "FIRST_HALF_REP_DIRECTION": self.first_half_rep_direction,
            },
        }


@dataclass(frozen=True, slots=True)
class P3SignalProfile:
    full_time: PeriodOUSignal
    first_half: PeriodOUSignal | None
    ft_1h: FT1HSignal | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "FT": self.full_time.to_dict(),
            "1H": None if self.first_half is None else self.first_half.to_dict(),
            "FT_1H": None if self.ft_1h is None else self.ft_1h.to_dict(),
        }


__all__ = [
    "BookOUReading",
    "BookRelationSignal",
    "FT1HSignal",
    "LineStructureSignal",
    "P3SignalProfile",
    "PeriodOUSignal",
    "RepresentativeSignal",
]
