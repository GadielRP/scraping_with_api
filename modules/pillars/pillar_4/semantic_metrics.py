"""Checkpoint-aligned semantic series built with the canonical P2/P3 primitives."""

from __future__ import annotations

import re
from datetime import timezone
from decimal import Decimal
from typing import Any, Callable, Iterable, Sequence

from modules.pillars.pillar_2_side_market.metrics import (
    absolute_gap,
    pair_mean,
    relative_spread,
    side_edge,
)
from modules.pillars.pillar_3_totals_market_context.metrics import ou_edge

from .models import P4Point, P4SeriesInput
from .periods import normalize_token


def _timestamp(value) -> float:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc).timestamp()
    return value.timestamp()


def _slug(value: Any) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "NA").strip().upper())
    return token.strip("_") or "NA"


def _role(domain: str, choice_name: str) -> str | None:
    token = normalize_token(choice_name)
    if domain == "SIDE":
        if token in {"home", "1", "local", "team 1", "home team"}:
            return "LEFT"
        if token in {"away", "2", "visitor", "team 2", "away team"}:
            return "RIGHT"
    if domain == "TOTALS":
        if token in {"over", "o", "more"} or token.startswith("over "):
            return "LEFT"
        if token in {"under", "u", "less"} or token.startswith("under "):
            return "RIGHT"
    return None


def _point_map(series: P4SeriesInput) -> dict[int, P4Point]:
    return {
        int(point.target_minute): point
        for point in series.points
        if point.target_minute is not None
    }


def _semantic_id(template: P4SeriesInput, value_type: str, source_scope: str) -> str:
    return "_".join(
        _slug(value)
        for value in (
            template.domain,
            template.market_group,
            template.market_period,
            template.market_name,
            template.choice_group_key,
            source_scope,
            "CHECKPOINT_VIEW",
            value_type,
        )
    )


def _combine(
    left: P4SeriesInput,
    right: P4SeriesInput,
    *,
    value_type: str,
    choice_name: str,
    source_scope: str,
    formula: Callable[[Decimal, Decimal], Decimal],
    bookie_id: int | None = None,
    bookie_name: str | None = None,
    preserve_bookie_identity: bool = True,
    source: str | None = "derived",
    exchange_side: str | None = None,
) -> P4SeriesInput | None:
    left_points = _point_map(left)
    right_points = _point_map(right)
    common = sorted(set(left_points) & set(right_points), reverse=True)
    expected = tuple(
        sorted(
            set(left.expected_target_minutes) | set(right.expected_target_minutes),
            reverse=True,
        )
    )
    operative = min(expected) if expected else (min(common) if common else None)
    if operative is None or operative not in common:
        return None
    points: list[P4Point] = []
    for target in common:
        left_point = left_points[target]
        right_point = right_points[target]
        effective = max(
            (left_point.effective_at, right_point.effective_at),
            key=_timestamp,
        )
        availability = max(
            (left_point.availability_at, right_point.availability_at),
            key=_timestamp,
        )
        distances = [
            value
            for value in (
                left_point.distance_from_target_minutes,
                right_point.distance_from_target_minutes,
            )
            if value is not None
        ]
        points.append(
            P4Point(
                point_id=(
                    f"{_slug(value_type)}_TARGET_{target}_"
                    f"{left_point.point_id}_{right_point.point_id}"
                ),
                value=formula(left_point.value, right_point.value),
                effective_at=effective,
                availability_at=availability,
                minutes_before_start=min(
                    left_point.minutes_before_start,
                    right_point.minutes_before_start,
                ),
                observation_kind=(
                    "OPERATIVE_ENDPOINT" if target == operative else "CHECKPOINT"
                ),
                target_minute=target,
                distance_from_target_minutes=max(distances) if distances else None,
            )
        )
    series_id = _semantic_id(left, value_type, source_scope)
    return P4SeriesInput(
        series_id=series_id,
        base_series_id=series_id.removesuffix(f"_{_slug(value_type)}"),
        domain=left.domain,
        view="CHECKPOINT_VIEW",
        value_type=value_type,
        market_id=left.market_id,
        market_group=left.market_group,
        market_period=left.market_period,
        market_name=left.market_name,
        choice_group=left.choice_group,
        choice_group_key=left.choice_group_key,
        choice_name=choice_name,
        choice_id=None,
        main_line=(
            True
            if left.main_line is True and right.main_line is True
            else None
        ),
        bookie_id=left.bookie_id if preserve_bookie_identity else bookie_id,
        bookie_name=(
            left.bookie_name
            if preserve_bookie_identity
            else bookie_name or "DERIVED"
        ),
        source=source,
        exchange_side=exchange_side,
        exchange_level=left.exchange_level,
        quote_id=None,
        points=tuple(points),
        expected_target_minutes=expected,
        missing_target_minutes=tuple(
            target for target in expected if target not in common
        ),
        diagnostics=(f"DERIVED_{value_type}",),
        constituent_series_ids=(left.series_id, right.series_id),
    )


def _edge_series(price_series: Sequence[P4SeriesInput]) -> list[P4SeriesInput]:
    groups: dict[tuple[Any, ...], dict[str, list[P4SeriesInput]]] = {}
    for series in price_series:
        role = _role(series.domain, series.choice_name)
        if role is None:
            continue
        key = (
            series.domain,
            series.market_id,
            series.market_group,
            series.market_period,
            series.market_name,
            series.choice_group_key,
            series.bookie_id,
            series.bookie_name,
            series.source,
            series.exchange_side,
            series.exchange_level,
        )
        groups.setdefault(key, {"LEFT": [], "RIGHT": []})[role].append(series)

    result: list[P4SeriesInput] = []
    for candidates in groups.values():
        if len(candidates["LEFT"]) != 1 or len(candidates["RIGHT"]) != 1:
            continue
        left, right = candidates["LEFT"][0], candidates["RIGHT"][0]
        value_type = "SIDE_EDGE" if left.domain == "SIDE" else "OU_EDGE"
        edge = _combine(
            left,
            right,
            value_type=value_type,
            choice_name=value_type,
            source_scope=(
                f"BOOKIE_{left.bookie_id}_{left.exchange_side or 'SINGLE'}"
            ),
            formula=side_edge if left.domain == "SIDE" else ou_edge,
            source=left.source,
            exchange_side=left.exchange_side,
        )
        if edge is not None:
            result.append(edge)
    return result


def _aggregate_edges(edge_series: Sequence[P4SeriesInput]) -> list[P4SeriesInput]:
    groups: dict[tuple[Any, ...], list[P4SeriesInput]] = {}
    for series in edge_series:
        key = (
            series.domain,
            series.market_id,
            series.market_group,
            series.market_period,
            series.market_name,
            series.choice_group_key,
        )
        groups.setdefault(key, []).append(series)

    result: list[P4SeriesInput] = []
    for related in groups.values():
        books = {
            series.bookie_id: series
            for series in related
            if series.bookie_id in {302, 3} and series.exchange_side is None
        }
        exchange = {
            normalize_token(series.exchange_side): series
            for series in related
            if series.bookie_id == 4 and series.exchange_side
        }
        book_rep = None
        exchange_rep = None
        if 302 in books and 3 in books:
            book_rep = _combine(
                books[302],
                books[3],
                value_type="BOOK_REP_EDGE",
                choice_name="BOOK_REP_EDGE",
                source_scope="BOOKS_REPRESENTATIVE",
                formula=pair_mean,
                bookie_name="BOOKS_REPRESENTATIVE",
                preserve_bookie_identity=False,
            )
            book_gap = _combine(
                books[302],
                books[3],
                value_type="BOOK_INTERNAL_GAP",
                choice_name="BOOK_INTERNAL_GAP",
                source_scope="BOOKS",
                formula=absolute_gap,
                bookie_name="BOOKS",
                preserve_bookie_identity=False,
            )
            result.extend(item for item in (book_rep, book_gap) if item is not None)
        if "back" in exchange and "lay" in exchange:
            exchange_rep = _combine(
                exchange["back"],
                exchange["lay"],
                value_type="EXCHANGE_REP_EDGE",
                choice_name="EXCHANGE_REP_EDGE",
                source_scope="BETFAIR_REPRESENTATIVE",
                formula=pair_mean,
                bookie_id=4,
                bookie_name="Betfair",
                preserve_bookie_identity=False,
                source="derived",
                exchange_side="representative",
            )
            exchange_gap = _combine(
                exchange["back"],
                exchange["lay"],
                value_type="EXCHANGE_INTERNAL_GAP",
                choice_name="EXCHANGE_INTERNAL_GAP",
                source_scope="BETFAIR",
                formula=absolute_gap,
                bookie_id=4,
                bookie_name="Betfair",
                preserve_bookie_identity=False,
                source="derived",
                exchange_side="back_lay_gap",
            )
            result.extend(item for item in (exchange_rep, exchange_gap) if item is not None)
        if book_rep is not None and exchange_rep is not None:
            cross_gap = _combine(
                book_rep,
                exchange_rep,
                value_type="BOOK_EXCHANGE_GAP",
                choice_name="BOOK_EXCHANGE_GAP",
                source_scope="BOOK_EXCHANGE",
                formula=absolute_gap,
                bookie_name="BOOK_EXCHANGE",
                preserve_bookie_identity=False,
            )
            if cross_gap is not None:
                result.append(cross_gap)
    return result


def _exchange_spreads(price_series: Sequence[P4SeriesInput]) -> list[P4SeriesInput]:
    groups: dict[tuple[Any, ...], dict[str, list[P4SeriesInput]]] = {}
    for series in price_series:
        side = normalize_token(series.exchange_side)
        if series.bookie_id != 4 or side not in {"back", "lay"}:
            continue
        key = (
            series.domain,
            series.market_id,
            series.market_group,
            series.market_period,
            series.market_name,
            series.choice_group_key,
            normalize_token(series.choice_name),
        )
        groups.setdefault(key, {"back": [], "lay": []})[
            side
        ].append(series)
    result: list[P4SeriesInput] = []
    for candidates in groups.values():
        if len(candidates.get("back", [])) != 1 or len(candidates.get("lay", [])) != 1:
            continue
        back, lay = candidates["back"][0], candidates["lay"][0]
        spread = _combine(
            back,
            lay,
            value_type="BACK_LAY_RELATIVE_SPREAD",
            choice_name=f"{back.choice_name}_BACK_LAY_RELATIVE_SPREAD",
            source_scope=f"BETFAIR_{back.choice_name}",
            formula=relative_spread,
            bookie_id=4,
            bookie_name="Betfair",
            source="derived",
            exchange_side="back_lay",
        )
        if spread is not None:
            result.append(spread)
    return result


def build_checkpoint_semantic_series(
    series_inputs: Iterable[P4SeriesInput],
) -> tuple[P4SeriesInput, ...]:
    prices = tuple(
        series
        for series in series_inputs
        if series.view == "CHECKPOINT_VIEW" and series.value_type == "ODDS_PRICE"
    )
    edges = _edge_series(prices)
    return tuple((*edges, *_aggregate_edges(edges), *_exchange_spreads(prices)))


__all__ = ["build_checkpoint_semantic_series"]
