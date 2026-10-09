"""Shared event index, capabilities, selection and coverage for P2–P5.

Views retain references to trajectories; no snapshots are copied or cached
outside the lifetime of the event evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from types import MappingProxyType
from typing import Iterable

from .evaluation_contracts import CoverageCell
from .odds_trajectory_context import OddsTrajectoryContext, MarketLineOddsTrajectory
from .trajectory_selection import TargetMinuteSelection
from .trajectory_sampling import TrajectorySamplingPolicy, select_line_checkpoints


@dataclass(frozen=True, slots=True)
class Bookmaker:
    name: str
    id: int
    exchange: bool = False


PINNACLE = Bookmaker("pinnacle", 302)
BET365 = Bookmaker("bet365", 3)
BETFAIR = Bookmaker("betfair", 4, True)
SOFASCORE = Bookmaker("sofascore", 1)
BOOKMAKERS = (PINNACLE, BET365, BETFAIR)
P5_BOOKMAKERS = (PINNACLE, BET365, SOFASCORE)
SUPPORTED_BOOKMAKERS = (*BOOKMAKERS, SOFASCORE)
BOOKMAKER_IDS = frozenset(b.id for b in SUPPORTED_BOOKMAKERS)
FT = "Full Time"
FT_OT = "Full Time Including Overtime"
FIRST_HALF = "1st Half"
FIRST_FIVE = "1st to 5th Inning"
CAPABILITIES = MappingProxyType(
    {
        2: (
            ("1X2", "Home/Away", "Asian Handicap", "Handicap"),
            (FT_OT, FT, FIRST_HALF, FIRST_FIVE),
        ),
        3: (("Over/Under",), (FT_OT, FT, FIRST_HALF)),
        4: (
            ("1X2", "Home/Away", "Asian Handicap", "Over/Under"),
            (FT_OT, FT, FIRST_HALF, "1st Quarter"),
        ),
        5: (("1X2", "Home/Away"), (FT_OT, FT)),
    }
)
CANONICAL_CONTRACTS = MappingProxyType(
    {
        "1x2_full_time": ("1X2", FT),
        "1x2_1st_half": ("1X2", FIRST_HALF),
        "1x2_first_to_fifth_inning": ("1X2", FIRST_FIVE),
        "home_away_full_time": ("Home/Away", FT),
        "home_away_1st_half": ("Home/Away", FIRST_HALF),
        "home_away_full_time_including_overtime": ("Home/Away", FT_OT),
        "home_away_first_to_fifth_inning": ("Home/Away", FIRST_FIVE),
        "over_under_full_time": ("Over/Under", FT),
        "over_under_1st_half": ("Over/Under", FIRST_HALF),
        "over_under_full_time_including_overtime": ("Over/Under", FT_OT),
        "over_under_1st_quarter": ("Over/Under", "1st Quarter"),
        "asian_handicap_full_time": ("Asian Handicap", FT),
        "asian_handicap_1st_half": ("Asian Handicap", FIRST_HALF),
        "asian_handicap_full_time_including_overtime": ("Asian Handicap", FT_OT),
        "handicap_full_time_including_overtime": ("Handicap", FT_OT),
        "handicap_first_to_fifth_inning": ("Handicap", FIRST_FIVE),
    }
)


@dataclass(frozen=True, slots=True)
class ReadingCapability:
    pillar: int
    name: str
    families: tuple[str, ...]
    outcomes: tuple[str, ...]
    book_ids: tuple[int, ...]
    periods: tuple[str, ...] = ()

    def supports_period(self, period: str) -> bool:
        return period in (self.periods or CAPABILITIES[self.pillar][1])


READING_CAPABILITIES = (
    ReadingCapability(
        2,
        "HOME_AWAY_EDGE",
        ("1X2", "Home/Away", "Asian Handicap", "Handicap"),
        ("1", "2"),
        (PINNACLE.id, BET365.id),
    ),
    ReadingCapability(
        2,
        "EXCHANGE_HOME_AWAY_EDGE",
        ("1X2", "Home/Away"),
        ("1", "2"),
        (BETFAIR.id,),
        periods=(FT_OT, FT),
    ),
    ReadingCapability(
        2,
        "EXCHANGE_HANDICAP_EDGE",
        ("Asian Handicap", "Handicap"),
        ("1", "2"),
        (BETFAIR.id,),
    ),
    ReadingCapability(
        3,
        "OVER_UNDER_EDGE",
        ("Over/Under",),
        ("over", "under"),
        (PINNACLE.id, BET365.id, BETFAIR.id),
    ),
    ReadingCapability(
        4,
        "TEMPORAL_MOVEMENT",
        CAPABILITIES[4][0],
        (),
        (PINNACLE.id, BET365.id, BETFAIR.id),
    ),
    ReadingCapability(
        5,
        "CURRENT_THREE_WAY_VECTOR",
        ("1X2",),
        ("1", "x", "2"),
        tuple(book.id for book in P5_BOOKMAKERS),
    ),
    ReadingCapability(
        5,
        "CURRENT_TWO_WAY_VECTOR",
        ("Home/Away",),
        ("1", "2"),
        tuple(book.id for book in P5_BOOKMAKERS),
    ),
)


def valid_price(value: object) -> bool:
    if value is None or isinstance(value, bool):
        return False
    try:
        price = Decimal(str(value))
        return price.is_finite() and price > 1
    except (InvalidOperation, ValueError, TypeError):
        return False


def contract_key(line: MarketLineOddsTrajectory) -> str:
    identity = (
        line.canonical_market_key,
        line.market_group,
        line.market_period,
        line.line_value,
        line.is_live,
    )
    return "contract_" + sha256(repr(identity).encode()).hexdigest()[:24]


def context_view(
    context: OddsTrajectoryContext, lines: Iterable[MarketLineOddsTrajectory]
) -> OddsTrajectoryContext:
    markets: dict = {}
    index: dict = {}
    for line in lines:
        markets.setdefault(line.market_group, {}).setdefault(
            line.market_period, {}
        ).setdefault(line.market_name, {})[
            f"{line.line_value}:{line.market_id}:{contract_key(line)}"
        ] = line
        index.setdefault(
            (line.market_group.casefold(), line.market_period.casefold()), []
        ).append(line)
    return replace(
        context, markets=markets, market_index={k: tuple(v) for k, v in index.items()}
    )


@dataclass(frozen=True, slots=True)
class EventMarketEvaluation:
    context: OddsTrajectoryContext
    target_selection: TargetMinuteSelection
    lines: tuple[MarketLineOddsTrajectory, ...]
    selected_full_time_period: str | None
    selection: dict
    diagnostics: tuple[dict, ...] = ()
    rejected_lines: tuple[MarketLineOddsTrajectory, ...] = ()

    def view(
        self,
        pillar: int,
        *,
        moneyline_family: str | None = None,
        secondary_period: str = FIRST_HALF,
    ) -> OddsTrajectoryContext:
        families, periods = CAPABILITIES[pillar]
        selected = []
        for line in self.lines:
            if line.market_group not in families or line.market_period not in periods:
                continue
            if (
                line.market_period in (FT, FT_OT)
                and line.market_period != self.selected_full_time_period
            ):
                continue
            if pillar != 4 and line.market_period not in (FT, FT_OT, secondary_period):
                continue
            if (
                moneyline_family
                and line.market_group in ("1X2", "Home/Away")
                and line.market_group != moneyline_family
            ):
                continue
            selected.append(line)
        return context_view(self.context, selected)

    def contracts(self, pillar: int) -> dict:
        families, periods = CAPABILITIES[pillar]
        rejected_ids = {id(line) for line in self.rejected_lines}
        result = {}
        for line in (*self.lines, *self.rejected_lines):
            if id(line) not in rejected_ids and (
                line.market_group not in families or line.market_period not in periods
            ):
                continue
            key = contract_key(line)
            contract = result.setdefault(
                key,
                {
                    "market_type_id": line.market_type_id,
                    "canonical_market_key": line.canonical_market_key,
                    "market_group": line.market_group,
                    "market_period": line.market_period,
                    "line_value": line.line_value,
                    "is_live": line.is_live,
                    "market_id": line.market_id,
                    "market_ids": [],
                    "bookie_ids": [],
                },
            )
            venues = {
                book.market_id or line.market_id for book in line.bookies.values()
            }
            contract["market_ids"] = sorted(
                set(contract["market_ids"]) | (venues - {None})
            )
            contract["market_id"] = (
                contract["market_ids"][0] if len(contract["market_ids"]) == 1 else None
            )
            contract["bookie_ids"] = sorted(
                set(contract["bookie_ids"])
                | {
                    book.bookie_id
                    for book in line.bookies.values()
                    if book.bookie_id in BOOKMAKER_IDS
                }
            )
        return result

    def coverage(self, pillar: int) -> tuple[CoverageCell, ...]:
        from .market_snapshot_extractor import (
            MarketSnapshotRequest,
            MarketIdentity,
            ChoiceRequest,
            extract_market_snapshot,
        )
        from .market_candidate_selection import select_market_candidate

        families, periods = CAPABILITIES[pillar]
        cells = []
        coverage_bookmakers = (
            (*BOOKMAKERS, SOFASCORE) if pillar == 5 else BOOKMAKERS
        )
        for book in coverage_bookmakers:
            for family in families:
                for period in periods:
                    observed = [
                        line
                        for line in self.lines
                        if line.market_group == family and line.market_period == period
                    ]
                    variants = sorted(
                        {line.line_value for line in observed}, key=lambda v: str(v)
                    ) or [None]
                    for variant in variants:
                        lines = [
                            line for line in observed if line.line_value == variant
                        ]
                        for side in (("back", "lay") if book.exchange else (None,)):
                            required = (
                                ("over", "under")
                                if family == "Over/Under"
                                else ("1", "x", "2") if family == "1X2" else ("1", "2")
                            )
                            request = MarketSnapshotRequest(
                                tuple(
                                    MarketIdentity(
                                        l.market_group, l.market_period, l.market_name
                                    )
                                    for l in lines
                                ),
                                book.id,
                                tuple(ChoiceRequest(o, o, o) for o in required),
                                line_input_name=(
                                    "line"
                                    if family not in ("1X2", "Home/Away")
                                    else None
                                ),
                                exchange_side=side,
                            )
                            extraction = extract_market_snapshot(
                                context_view(self.context, lines),
                                target_minute=self.target_selection.target_minute,
                                request=request,
                            )
                            selection = select_market_candidate(extraction, request)
                            present = any(
                                b.bookie_id == book.id and b.exchange_side == side
                                for line in lines
                                for b in line.bookies.values()
                            )
                            status = (
                                "AMBIGUOUS"
                                if selection.ambiguous
                                else (
                                    "INVALID"
                                    if selection.invalid
                                    else (
                                        "COMPLETE"
                                        if selection.candidate is not None
                                        and selection.candidate.is_complete(request)
                                        else "INCOMPLETE" if present else "MISSING"
                                    )
                                )
                            )
                            reason = None
                            observed_status = status
                            if (
                                period in (FT, FT_OT)
                                and period != self.selected_full_time_period
                            ):
                                status, reason = "EXCLUDED", "UNSELECTED_PERIOD"
                            if (
                                (family, period) not in CANONICAL_CONTRACTS.values()
                                or (pillar == 5 and book.exchange)
                                or not any(
                                    cap.pillar == pillar
                                    and book.id in cap.book_ids
                                    and family in cap.families
                                    and cap.supports_period(period)
                                    for cap in READING_CAPABILITIES
                                )
                            ):
                                status, reason = "NOT_APPLICABLE", (
                                    "DIAGNOSTIC_ONLY"
                                    if pillar == 5 and book.exchange
                                    else (
                                        "UNSUPPORTED_CONTRACT"
                                        if (family, period)
                                        not in CANONICAL_CONTRACTS.values()
                                        else "UNSUPPORTED_READING"
                                    )
                                )
                            refs = tuple(
                                contract_key(line)
                                for line in lines
                                if any(
                                    b.bookie_id == book.id and b.exchange_side == side
                                    for b in line.bookies.values()
                                )
                            )
                            cells.append(
                                CoverageCell(
                                    f"{book.id}:{family}:{period}:{variant}:{side}",
                                    book.id,
                                    family,
                                    period,
                                    status,
                                    refs,
                                    tuple(sorted(selection.missing)),
                                    reason,
                                    variant,
                                    side,
                                    observed_status,
                                )
                            )
        for line in self.rejected_lines:
            for book in line.bookies.values():
                if book.bookie_id in BOOKMAKER_IDS:
                    cells.append(
                        CoverageCell(
                            f"excluded:{contract_key(line)}:{book.bookie_id}:{book.exchange_side}",
                            book.bookie_id,
                            line.market_group,
                            line.market_period,
                            "EXCLUDED",
                            (contract_key(line),),
                            reason="UNSUPPORTED_CONTRACT",
                            line_value=line.line_value,
                            exchange_side=book.exchange_side,
                        )
                    )
        return tuple(cells)


def _valid_line(value: object) -> bool:
    try:
        return value is not None and Decimal(str(value)).is_finite()
    except (InvalidOperation, ValueError, TypeError):
        return False


def prepare_event_markets(
    context: OddsTrajectoryContext | None,
    target_selection: TargetMinuteSelection,
    event_context=None,
) -> EventMarketEvaluation:
    if context is None:
        context = OddsTrajectoryContext(
            False, getattr(event_context, "event_id", None), [], [], []
        )
    lines, diagnostics, rejected = [], [], []
    for periods in context.markets.values():
        for names in periods.values():
            for containers in names.values():
                for line in containers.values():
                    pair = (line.market_group, line.market_period)
                    canonical = (
                        CANONICAL_CONTRACTS.get(line.canonical_market_key)
                        if line.canonical_market_key
                        else pair if pair in CANONICAL_CONTRACTS.values() else None
                    )
                    if line.is_live or canonical != pair:
                        rejected.append(line)
                        diagnostics.append(
                            {
                                "reason": "UNSUPPORTED_CONTRACT",
                                "contract": contract_key(line),
                            }
                        )
                        continue
                    books = {
                        k: b
                        for k, b in line.bookies.items()
                        if b.bookie_id in BOOKMAKER_IDS
                    }
                    if books:
                        canonical_key = line.canonical_market_key or next(
                            key
                            for key, value in CANONICAL_CONTRACTS.items()
                            if value == pair
                        )
                        lines.append(
                            replace(
                                line, bookies=books, canonical_market_key=canonical_key
                            )
                        )
    candidates = []
    minute = target_selection.target_minute
    sampling = (
        TrajectorySamplingPolicy.for_context(context, minute, event_context.starts_at)
        if minute is not None and event_context is not None else None
    )
    from .market_snapshot_extractor import (
        MarketSnapshotRequest,
        MarketIdentity,
        ChoiceRequest,
        extract_market_snapshot,
    )
    from .market_candidate_selection import select_market_candidate

    for period in (FT_OT, FT):
        useful, blocked = [], []
        period_lines = [line for line in lines if line.market_period == period]
        for family in sorted({line.market_group for line in period_lines}):
            family_lines = [
                line for line in period_lines if line.market_group == family
            ]
            useful.extend(_line_readings(context, family_lines, minute, sampling))
            for variant in sorted({line.line_value for line in family_lines}, key=str):
                scoped_lines = [
                    line for line in family_lines if line.line_value == variant
                ]
                view = context_view(context, scoped_lines)
                for book in SUPPORTED_BOOKMAKERS:
                    supported = [
                        cap
                        for cap in READING_CAPABILITIES
                        if family in cap.families
                        and book.id in cap.book_ids
                        and cap.outcomes
                        and cap.supports_period(period)
                    ]
                    for side in (("back", "lay") if book.exchange else (None,)):
                        for capability in supported:
                            request = MarketSnapshotRequest(
                                tuple(
                                    MarketIdentity(
                                        l.market_group, l.market_period, l.market_name
                                    )
                                    for l in scoped_lines
                                ),
                                book.id,
                                tuple(
                                    ChoiceRequest(o, o, o) for o in capability.outcomes
                                ),
                                exchange_side=side,
                            )
                            result = select_market_candidate(
                                extract_market_snapshot(
                                    view, target_minute=minute, request=request
                                ),
                                request,
                                allow_partial=False,
                            )
                            if result.candidate is not None:
                                useful.append(
                                    contract_key(result.candidate.market_line)
                                )
                            elif any(
                                b.bookie_id == book.id and b.exchange_side == side
                                for line in scoped_lines
                                for b in line.bookies.values()
                            ):
                                blocked.append(
                                    {
                                        "reading": capability.name,
                                        "pillar": capability.pillar,
                                        "family": family,
                                        "bookie_id": book.id,
                                        "line_value": variant,
                                        "exchange_side": side,
                                        "reason": (
                                            "AMBIGUOUS_CANDIDATE"
                                            if result.ambiguous
                                            else (
                                                "INVALID_VALUE"
                                                if result.invalid
                                                else "MISSING_INPUT"
                                            )
                                        ),
                                        "missing": sorted(result.missing),
                                        "invalid": sorted(result.invalid),
                                    }
                                )
                temporal = next(
                    (
                        cap
                        for cap in READING_CAPABILITIES
                        if cap.pillar == 4 and family in cap.families
                    ),
                    None,
                )
                if temporal and sampling is not None:
                    for line in scoped_lines:
                        if contract_key(line) in useful:
                            continue
                        for book in line.bookies.values():
                            if book.bookie_id in temporal.book_ids and any(
                                choice.main_line is not False
                                and sampling.select(choice.snapshots).has_movement
                                for choice in book.choices.values()
                            ):
                                useful.append(contract_key(line))
        candidates.append(
            {
                "period": period,
                "usable": bool(useful) and minute is not None,
                "contract_refs": sorted(set(useful)),
                "reason": None if useful else "NO_CURRENT_READING",
                "blocked_readings": blocked,
            }
        )
    selected = next((c["period"] for c in candidates if c["usable"]), None)
    selection = {
        "selected_period": selected,
        "candidates": candidates,
        "reason": (
            "OVERTIME_PRIORITY"
            if selected == FT_OT
            else "REGULATION_FALLBACK" if selected == FT else "NO_USABLE_FULL_TIME"
        ),
        "target_selection": {
            "reason": target_selection.reason,
            "diagnostics": target_selection.diagnostics,
        },
        "checkpoint": {
            "target_minute": minute,
            "evaluation_as_of": (
                context.evaluation_as_of.isoformat()
                if context.evaluation_as_of
                else None
            ),
        },
    }
    return EventMarketEvaluation(
        context,
        target_selection,
        tuple(lines),
        selected,
        selection,
        tuple(diagnostics),
        tuple(rejected),
    )


def _line_readings(context, lines, minute, sampling):
    """Existing line separation and P4 transitions need lines, not price pairs."""
    if (
        not lines
        or minute is None
        or lines[0].market_group not in ("Over/Under", "Asian Handicap", "Handicap")
    ):
        return ()
    from .market_snapshot_extractor import (
        MarketSnapshotRequest,
        MarketIdentity,
        extract_market_snapshot,
    )
    from .market_candidate_selection import select_market_candidate

    family = lines[0].market_group
    view = context_view(context, lines)
    identities = tuple(
        MarketIdentity(line.market_group, line.market_period, line.market_name)
        for line in lines
    )
    representatives = []
    for book in (PINNACLE, BET365):
        request = MarketSnapshotRequest(identities, book.id, (), line_input_name="line")
        selection = select_market_candidate(
            extract_market_snapshot(view, target_minute=minute, request=request),
            request,
            allow_partial=False,
        )
        # A line is current only if it has an observation at the selected checkpoint.
        candidate = selection.candidate
        if candidate and any(
            minute in choice.odds_values for choice in candidate.bookie.choices.values()
        ):
            representatives.append(contract_key(candidate.market_line))
    useful = representatives if len(representatives) == 2 else []
    if useful or family == "Handicap" or sampling is None:
        return tuple(useful)
    related = {}
    temporal_books = {
        book
        for capability in READING_CAPABILITIES
        if capability.pillar == 4
        for book in capability.book_ids
    }
    for line in lines:
        if not _valid_line(line.line_value):
            continue
        for book in line.bookies.values():
            if book.bookie_id not in temporal_books:
                continue
            for choice in book.choices.values():
                if choice.main_line is False:
                    continue
                key = (
                    book.bookie_id,
                    book.source,
                    book.exchange_side,
                    book.exchange_level,
                )
                related.setdefault(key, []).append((line, choice))
    for members in related.values():
        projected = [
            (line, sampling.select(choice.snapshots).checkpoints)
            for line, choice in members
        ]
        selected, _ = select_line_checkpoints(
            ((line.line_value, points) for line, points in projected),
            minute, tuple(sampling.windows),
        )
        if minute in selected and len(selected) >= 2:
            useful.extend(
                contract_key(line) for line, points in projected
                if minute in points and Decimal(str(line.line_value)) == selected[minute][0]
            )
    return tuple(useful)
