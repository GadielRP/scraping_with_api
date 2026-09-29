"""Compact, readable diagnostics for one Pillar 4 evaluation."""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from .models import P4ExtractionResult
from .periods import REQUIRED_BOOKIE_IDS, bookmaker_name, period_key, resolve_domain


def _time(value: str | None) -> str:
    if value is None:
        return "none"
    return datetime.fromisoformat(value).astimezone(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def _number(value: Any) -> str:
    if value is None:
        return "unavailable"
    return f"{float(value):g}"


def _signed_number(value: Any) -> str:
    if value is None:
        return "unavailable"
    return f"{float(value):+g}"


def _market_label(market: dict[str, Any]) -> str:
    name = str(market.get("MARKET_NAME") or "unknown market")
    line = market.get("CHOICE_GROUP")
    return f"{name} (line {line})" if line is not None else name


def _choice_label(market: dict[str, Any]) -> str:
    value_type = market.get("VALUE_TYPE")
    if value_type == "SIDE_EDGE":
        return "home vs away"
    if value_type == "OU_EDGE":
        return "over vs under"
    choice = str(market.get("CHOICE_NAME") or "unknown")
    if market.get("DOMAIN") == "SIDE":
        return {"1": "home (1)", "2": "away (2)", "x": "draw (X)"}.get(
            choice.lower(), choice
        )
    return choice


def _pattern_label(value: str | None) -> str:
    return {
        "NO_MOVEMENT": "unchanged",
        "UNIDIRECTIONAL": "one direction",
        "REVERSAL": "direction changed once",
        "MULTI_REVERSAL": "direction changed several times",
    }.get(value, "unavailable")


def _missing_by_period(extraction: P4ExtractionResult) -> Counter[str]:
    counts: Counter[str] = Counter()
    for detail in extraction.missing_endpoint_details:
        domain = resolve_domain(detail["market_group"], detail["market_name"])
        if domain is not None:
            counts[f"{domain}_{period_key(detail['market_period'])}"] += 1
    return counts


def log_p4_extraction(
    logger: logging.Logger,
    extraction: P4ExtractionResult,
) -> None:
    """Explain the cutoff, missing selections, and coverage without dumping IDs."""
    target = extraction.target_minute
    logger.info(
        "P4 input | event=%s | target=T-%s | nominal=%s | evaluation=%s | "
        "data cutoff=%s | usable=%s",
        extraction.event_id,
        target,
        _time(
            extraction.nominal_target_as_of.isoformat()
            if extraction.nominal_target_as_of is not None
            else None
        ),
        _time(
            extraction.evaluation_as_of.isoformat()
            if extraction.evaluation_as_of is not None
            else None
        ),
        _time(
            extraction.operative_as_of.isoformat()
            if extraction.operative_as_of is not None
            else None
        ),
        "yes" if extraction.usable else "no",
    )
    logger.info(
        "P4 input | selections examined=%s | with T-%s quote=%s | "
        "without T-%s quote=%s | later snapshots ignored=%s | "
        "invalid observations=%s | ambiguous lines=%s",
        extraction.source_series_seen,
        target,
        extraction.endpoint_series_present,
        target,
        len(extraction.missing_inputs),
        extraction.excluded_future_points,
        len(extraction.invalid_inputs),
        len(extraction.ambiguous_inputs),
    )

    missing_by_period = _missing_by_period(extraction)
    for period, diagnostics in sorted(extraction.periods.items()):
        logger.info(
            "P4 market | %s / %s | status=%s | "
            "built adaptive series=%s, checkpoint series=%s | "
            "incomplete required series=%s, optional series=%s (all=%s) | "
            "selections omitted for missing T-%s quote=%s",
            "sides" if diagnostics["DOMAIN"] == "SIDE" else "totals",
            diagnostics["MARKET_PERIOD"].replace("_", " ").lower(),
            diagnostics["status"],
            diagnostics["ADAPTIVE_SERIES_COUNT"],
            diagnostics["CHECKPOINT_SERIES_COUNT"],
            diagnostics["PARTIAL_REQUIRED_SERIES_COUNT"],
            diagnostics["PARTIAL_OPTIONAL_SERIES_COUNT"],
            diagnostics["PARTIAL_SERIES_COUNT"],
            target,
            missing_by_period[period],
        )

    groups: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    for detail in extraction.missing_endpoint_details:
        key = (
            detail["market_name"],
            detail["market_period"],
            detail["line_value"],
            detail["bookie_name"],
            detail["exchange_side"],
            detail["last_available_at"],
            detail["first_after_cutoff_at"],
        )
        groups[key].append(str(detail["choice_name"]))
    for key, choices in sorted(groups.items(), key=lambda item: str(item[0])):
        market, period, line, bookie, side, last_before, first_after = key
        market_label = f"{market} (line {line})" if line is not None else market
        source = f"{bookie} {side}" if side else bookie
        logger.info(
            "P4 missing T-%s quote | market=%s, period=%s | source=%s | "
            "choices=%s | last available by cutoff=%s | first later observation=%s",
            target,
            market_label,
            period,
            source,
            ", ".join(sorted(set(choices))),
            _time(last_before),
            _time(first_after),
        )
    if extraction.invalid_inputs or extraction.ambiguous_inputs:
        logger.info(
            "P4 input | detailed invalid and ambiguous identifiers are in "
            "raw.extraction_diagnostics"
        )


def log_p4_signal_profile(
    logger: logging.Logger,
    profile: dict[str, Any],
    extraction: P4ExtractionResult,
) -> None:
    """Show the price paths and outcome in a bounded number of log lines."""
    summary = profile["SUMMARY"]
    primary = [
        series
        for view_name in ("ADAPTIVE_VIEW", "CHECKPOINT_VIEW")
        for series in (profile.get(view_name) or {}).get("SERIES", ())
        if (series.get("MARKET") or {}).get("VALUE_TYPE") == "ODDS_PRICE"
    ]
    incomplete_required = sum(
        series.get("STATUS") != "ACTIVE"
        and (series.get("MARKET") or {}).get("BOOKIE_ID") in REQUIRED_BOOKIE_IDS
        for series in primary
    )
    incomplete_optional = sum(
        series.get("STATUS") != "ACTIVE"
        and (series.get("MARKET") or {}).get("BOOKIE_ID") not in REQUIRED_BOOKIE_IDS
        for series in primary
    )
    missing_required_sources = [
        f"{bookmaker_name(bookie_id)} (id={bookie_id})"
        for bookie_id in sorted(
            REQUIRED_BOOKIE_IDS - set(extraction.observed_bookie_ids)
        )
    ]
    required_sources_with_issues = [
        f"{bookmaker_name(bookie_id)} (id={bookie_id})"
        for bookie_id in sorted(
            REQUIRED_BOOKIE_IDS & set(extraction.issue_bookie_ids)
        )
    ]
    logger.info(
        "P4 result | status=%s | missing T-%s selections=%s | "
        "invalid observations=%s | ambiguous lines=%s | "
        "missing required sources=%s | required sources with issues=%s | "
        "incomplete required price series=%s | incomplete optional price series=%s",
        summary["STATUS"],
        extraction.target_minute,
        len(extraction.missing_inputs),
        len(extraction.invalid_inputs),
        len(extraction.ambiguous_inputs),
        missing_required_sources,
        required_sources_with_issues,
        incomplete_required,
        incomplete_optional,
    )
    for view_name, label in (
        ("ADAPTIVE_VIEW", "all available snapshots"),
        ("CHECKPOINT_VIEW", "fixed checkpoints"),
    ):
        view = profile.get(view_name)
        if view is None:
            logger.info("P4 view | %s | unavailable", label)
        else:
            logger.info(
                "P4 view | %s | status=%s | series=%s",
                label,
                view["STATUS"],
                len(view["SERIES"]),
            )

    checkpoint_series = (profile.get("CHECKPOINT_VIEW") or {}).get("SERIES", ())
    for series in checkpoint_series:
        market = series.get("MARKET") or {}
        value_type = market.get("VALUE_TYPE")
        if value_type not in {"ODDS_PRICE", "SIDE_EDGE", "OU_EDGE"}:
            continue
        observations = " -> ".join(
            f"T-{point['TARGET_MINUTE']}:{_number(point['VALUE'])}"
            for point in series.get("POINTS", ())
        ) or "none"
        features = series.get("RAW_TEMPORAL_FEATURES") or {}
        signals = series.get("STRUCTURAL_SIGNALS") or {}
        source = market.get("BOOKIE_NAME") or "derived"
        if market.get("EXCHANGE_SIDE"):
            source = f"{source} {market['EXCHANGE_SIDE']}"
        logger.info(
            "P4 trajectory | %s | %s | choice=%s | source=%s | "
            "values=%s | net=%s | pattern=%s | status=%s",
            "odds" if value_type == "ODDS_PRICE" else "market balance",
            _market_label(market),
            _choice_label(market),
            source,
            observations,
            _signed_number(features.get("NET_MOVE_RAW")),
            _pattern_label(signals.get("PATH_PATTERN_RAW")),
            series.get("STATUS"),
        )

    types = Counter(
        (series.get("MARKET") or {}).get("VALUE_TYPE")
        for series in checkpoint_series
    )
    logger.info(
        "P4 derived series | line series=%s | side balances=%s | "
        "totals balances=%s | bookmaker averages=%s | bookmaker gaps=%s",
        types["LINE"],
        types["SIDE_EDGE"],
        types["OU_EDGE"],
        types["BOOK_REP_EDGE"],
        types["BOOK_INTERNAL_GAP"],
    )
    relations = sum(
        (series.get("STRUCTURAL_SIGNALS") or {}).get(
            "BOOK_EXCHANGE_RELATION_CHANGE_RAW"
        )
        is not None
        for series in checkpoint_series
    )
    logger.info(
        "P4 book/exchange comparison | %s",
        f"available for {relations} series"
        if relations
        else "unavailable: relation change needs at least two shared book/exchange checkpoints",
    )


__all__ = ["log_p4_extraction", "log_p4_signal_profile"]
