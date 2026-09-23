"""Pure, deterministic P5 v3.0 scoring functions."""

from __future__ import annotations

import logging
from decimal import Decimal

from .calculation_models import (
    BookmakerMemoryProfile,
    MemoryQueryKey,
    MemorySample,
    historical_match_to_dict,
)

MIN_SAMPLE_SIZE = 3
SAMPLE_SATURATION = Decimal("8")
ZERO = Decimal("0")
ONE = Decimal("1")
logger = logging.getLogger(__name__)


def _fmt(value: object) -> str:
    return "None" if value is None else str(value)


def _log_formula(
    name: str,
    formula: str,
    substitution: str,
    result: object,
    *,
    debug_mode: bool,
) -> None:
    if debug_mode:
        logger.info("P5 FORMULA | %s | formula=%s", name, formula)
        logger.info("P5 FORMULA | %s | substitution=%s", name, substitution)
        logger.info("P5 FORMULA | %s | result=%s", name, _fmt(result))


def msri_signal(msri_raw: Decimal) -> Decimal:
    """Discretize MSRI_RAW using the canonical descending thresholds."""
    if msri_raw >= Decimal("0.60"):
        return Decimal("1.00")
    if msri_raw >= Decimal("0.40"):
        return Decimal("0.75")
    if msri_raw >= Decimal("0.20"):
        return Decimal("0.50")
    if msri_raw >= Decimal("0.10"):
        return Decimal("0.25")
    return ZERO


def sample_weight(sample_size: int) -> Decimal:
    """Return the post-bucket sample weight for a sufficient sample."""
    if sample_size < MIN_SAMPLE_SIZE:
        raise ValueError("sample weight is undefined below the minimum sample size")
    if sample_size <= 4:
        return Decimal("0.40")
    if sample_size <= 7:
        return Decimal("0.60")
    if sample_size <= 12:
        return Decimal("0.80")
    return ONE


def profile_strength(score: Decimal) -> str:
    if score < Decimal("0.10"):
        return "NONE"
    if score < Decimal("0.25"):
        return "WEAK"
    if score < Decimal("0.50"):
        return "MODERATE"
    return "STRONG"


def _profile_base(
    *,
    bookmaker: str,
    bookie_id: int,
    target_minute: int | None,
    key: MemoryQueryKey | None,
    p5_status: str,
    memory_status: str,
    reason: str | None,
    diagnostics: dict | None = None,
) -> dict:
    return {
        "bookmaker": bookmaker,
        "bookie_id": bookie_id,
        "p5_status": p5_status,
        "p5_valid": False,
        "p5_direction": "NONE",
        "p5": ZERO,
        "p5_strength": "NONE",
        "market_group": key.market_group if key else None,
        "market_period": key.market_period if key else None,
        "market_shape": key.market_shape if key else None,
        "target_minute": target_minute,
        "current_price_vector": key.price_vector() if key else None,
        "memory_status": memory_status,
        "reason": reason,
        "diagnostics": diagnostics or {},
    }


def not_eligible_profile(
    *,
    bookmaker: str,
    bookie_id: int,
    target_minute: int | None,
    reason: str,
    key: MemoryQueryKey | None = None,
    diagnostics: dict | None = None,
) -> BookmakerMemoryProfile:
    return BookmakerMemoryProfile(
        **_profile_base(
            bookmaker=bookmaker,
            bookie_id=bookie_id,
            target_minute=target_minute,
            key=key,
            p5_status="INSUFFICIENT_DATA",
            memory_status="NOT_ELIGIBLE",
            reason=reason,
            diagnostics=diagnostics,
        )
    )


def error_profile(
    *,
    bookmaker: str,
    bookie_id: int,
    target_minute: int | None,
    reason: str,
    key: MemoryQueryKey | None = None,
    diagnostics: dict | None = None,
) -> BookmakerMemoryProfile:
    return BookmakerMemoryProfile(
        **_profile_base(
            bookmaker=bookmaker,
            bookie_id=bookie_id,
            target_minute=target_minute,
            key=key,
            p5_status="ERROR",
            memory_status="ERROR",
            reason=reason,
            diagnostics=diagnostics,
        )
    )


def calculate_memory_profile(
    *,
    bookmaker: str,
    target_minute: int | None,
    sample: MemorySample,
    debug_mode: bool = False,
) -> BookmakerMemoryProfile:
    """Calculate one independent bookmaker profile from one complete sample."""
    if sample.sample_size != len(sample.historical_matches):
        raise ValueError("sample_size does not match historical_matches")
    if min(sample.wins_home, sample.wins_draw, sample.wins_away) < 0:
        raise ValueError("sample outcome counts cannot be negative")

    expected_total = sample.wins_home + sample.wins_away
    if sample.key.market_shape == "THREE_WAY":
        expected_total += sample.wins_draw
    elif sample.wins_draw != 0:
        raise ValueError("TWO_WAY sample cannot contain draw outcomes")
    if expected_total != sample.sample_size:
        raise ValueError("sample outcome counts do not add up to sample_size")

    if debug_mode:
        logger.info(
            "P5 FORMULA | %s | sample_size=%s outcomes(home=%s, draw=%s, away=%s) market_shape=%s",
            bookmaker,
            sample.sample_size,
            sample.wins_home,
            sample.wins_draw,
            sample.wins_away,
            sample.key.market_shape,
        )

    history = tuple(
        historical_match_to_dict(match) for match in sample.historical_matches
    )
    common = {
        "bookmaker": bookmaker,
        "bookie_id": sample.key.bookie_id,
        "market_group": sample.key.market_group,
        "market_period": sample.key.market_period,
        "market_shape": sample.key.market_shape,
        "target_minute": target_minute,
        "current_price_vector": sample.key.price_vector(),
        "sample_size": sample.sample_size,
        "wins_home": sample.wins_home,
        "wins_draw": sample.wins_draw,
        "wins_away": sample.wins_away,
        "historical_matches": history,
        "diagnostics": {
            "eligibility": list(sample.eligibility_diagnostics),
            "query_key": sample.key.to_dict(),
        },
    }

    if sample.sample_size < MIN_SAMPLE_SIZE:
        logger.info(
            "P5 PROFILE | bookmaker=%s status=INSUFFICIENT_DATA reason=minimum_sample_size_not_met sample_size=%s minimum=%s",
            bookmaker,
            sample.sample_size,
            MIN_SAMPLE_SIZE,
        )
        return BookmakerMemoryProfile(
            **common,
            p5_status="INSUFFICIENT_DATA",
            p5_valid=False,
            p5_direction="NONE",
            p5=ZERO,
            p5_strength="NONE",
            memory_status="INSUFFICIENT_DATA",
            reason="minimum_sample_size_not_met",
        )

    counts = {
        "HOME": sample.wins_home,
        "AWAY": sample.wins_away,
    }
    if sample.key.market_shape == "THREE_WAY":
        counts["DRAW"] = sample.wins_draw

    wins_dominant = max(counts.values())
    dominant = [side for side, count in counts.items() if count == wins_dominant]
    baseline = ONE / Decimal(len(counts))
    continuous_sample_factor = min(
        Decimal(sample.sample_size) / SAMPLE_SATURATION,
        ONE,
    )
    weighted_sample = sample_weight(sample.sample_size)

    _log_formula(
        f"{bookmaker}.BASELINE",
        "1 / number_of_outcomes",
        f"1 / {len(counts)}",
        baseline,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.SAMPLE_FACTOR",
        "min(sample_size / SAMPLE_SATURATION, 1)",
        f"min({sample.sample_size} / {SAMPLE_SATURATION}, 1)",
        continuous_sample_factor,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.SAMPLE_WEIGHT",
        "sample_weight(sample_size)",
        f"sample_weight({sample.sample_size})",
        weighted_sample,
        debug_mode=debug_mode,
    )

    if len(dominant) != 1:
        logger.info(
            "P5 PROFILE | bookmaker=%s status=ACTIVE memory_status=TIE dominant_results=%s wins=%s",
            bookmaker,
            dominant,
            wins_dominant,
        )
        return BookmakerMemoryProfile(
            **common,
            p5_status="ACTIVE",
            p5_valid=True,
            p5_direction="NONE",
            p5=ZERO,
            p5_strength="NONE",
            memory_status="TIE",
            reason="dominant_result_tie",
            is_tie=True,
            dominant_result=None,
            wins_dominant=wins_dominant,
            baseline=baseline,
            sample_factor=continuous_sample_factor,
            sample_weight=weighted_sample,
        )

    direction = dominant[0]
    p_hist_dominant = Decimal(wins_dominant) / Decimal(sample.sample_size)
    hist_edge = (p_hist_dominant - baseline) / (ONE - baseline)
    consistency = Decimal("0.5") + Decimal("0.5") * hist_edge
    raw = hist_edge * consistency * continuous_sample_factor
    signal = msri_signal(raw)
    score = signal * weighted_sample

    _log_formula(
        f"{bookmaker}.P_HIST_DOMINANT",
        "wins_dominant / sample_size",
        f"{wins_dominant} / {sample.sample_size}",
        p_hist_dominant,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.HIST_EDGE",
        "(p_hist_dominant - baseline) / (1 - baseline)",
        f"({p_hist_dominant} - {baseline}) / (1 - {baseline})",
        hist_edge,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.CONSISTENCY",
        "0.5 + 0.5 * hist_edge",
        f"0.5 + 0.5 * {hist_edge}",
        consistency,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.MSRI_RAW",
        "hist_edge * consistency * sample_factor",
        f"{hist_edge} * {consistency} * {continuous_sample_factor}",
        raw,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.MSRI_SIGNAL",
        "threshold_bucket(msri_raw)",
        f"threshold_bucket({raw})",
        signal,
        debug_mode=debug_mode,
    )
    _log_formula(
        f"{bookmaker}.P5",
        "msri_signal * sample_weight",
        f"{signal} * {weighted_sample}",
        score,
        debug_mode=debug_mode,
    )
    logger.info(
        "P5 PROFILE | bookmaker=%s status=ACTIVE direction=%s score=%s strength=%s sample_size=%s",
        bookmaker,
        direction,
        score,
        profile_strength(score),
        sample.sample_size,
    )

    return BookmakerMemoryProfile(
        **common,
        p5_status="ACTIVE",
        p5_valid=True,
        p5_direction=direction,
        p5=score,
        p5_strength=profile_strength(score),
        memory_status="ACTIVE",
        reason=None,
        is_tie=False,
        dominant_result=direction,
        wins_dominant=wins_dominant,
        p_hist_dominant=p_hist_dominant,
        baseline=baseline,
        hist_edge=hist_edge,
        consistency=consistency,
        sample_factor=continuous_sample_factor,
        msri_raw=raw,
        msri_signal=signal,
        sample_weight=weighted_sample,
    )


__all__ = [
    "MIN_SAMPLE_SIZE",
    "calculate_memory_profile",
    "error_profile",
    "msri_signal",
    "not_eligible_profile",
    "profile_strength",
    "sample_weight",
]
