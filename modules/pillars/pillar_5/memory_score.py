"""Pure scoring formulas preserved by the P5 v4 execution contract."""

from __future__ import annotations

import logging
from decimal import Decimal
from modules.pillars.evaluation_contracts import (
    EXECUTIVE_SUMMARY_VERSION,
    INTERPRETATION_VERSION,
    interpretation_reading,
)

from .calculation_models import (
    BookmakerMemoryProfile,
    MemorySample,
)

MIN_SAMPLE_SIZE = 3
SAMPLE_SATURATION = Decimal("8")
MSRI_THRESHOLDS = (
    (Decimal("0.60"), Decimal("1.00")),
    (Decimal("0.40"), Decimal("0.75")),
    (Decimal("0.20"), Decimal("0.50")),
    (Decimal("0.10"), Decimal("0.25")),
)
SAMPLE_WEIGHT_BUCKETS = (
    (4, Decimal("0.40")),
    (7, Decimal("0.60")),
    (12, Decimal("0.80")),
)
STRENGTH_THRESHOLDS = (
    (Decimal("0.10"), "NONE"),
    (Decimal("0.25"), "WEAK"),
    (Decimal("0.50"), "MODERATE"),
)
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
    for threshold, value in MSRI_THRESHOLDS:
        if msri_raw >= threshold:
            return value
    return ZERO


def sample_weight(sample_size: int) -> Decimal:
    """Return the post-bucket sample weight for a sufficient sample."""
    if sample_size < MIN_SAMPLE_SIZE:
        raise ValueError("sample weight is undefined below the minimum sample size")
    for maximum, weight in SAMPLE_WEIGHT_BUCKETS:
        if sample_size <= maximum:
            return weight
    return ONE


def profile_strength(score: Decimal) -> str:
    for threshold, classification in STRENGTH_THRESHOLDS:
        if score < threshold:
            return classification
    return "STRONG"


def calculate_memory_profile(
    *,
    bookmaker: str,
    target_minute: int | None,
    sample: MemorySample,
    debug_mode: bool = False,
) -> BookmakerMemoryProfile:
    """Calculate one independent bookmaker profile from one complete sample."""
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
        "sample_id": sample.sample_id,
        "diagnostics": {
            "exclusions": sample.exclusions,
            "query_key": sample.key.to_dict(),
        },
    }

    _log_formula(
        f"{bookmaker}.SAMPLE_SUFFICIENCY",
        "sample_size >= MIN_SAMPLE_SIZE",
        f"{sample.sample_size} >= {MIN_SAMPLE_SIZE}",
        sample.sample_size >= MIN_SAMPLE_SIZE,
        debug_mode=debug_mode,
    )

    if sample.sample_size < MIN_SAMPLE_SIZE:
        return BookmakerMemoryProfile(
            **common,
            p5_status="INSUFFICIENT_DATA",
            p5_valid=False,
            p5_direction="NONE",
            p5=None,
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


def interpret_p5_result(result):
    """Describe frozen historical counts without changing the memory score."""
    readings = {}
    signals = {signal["key"]: signal for signal in result["signals"]}
    for scope in dict.fromkeys((*result["analysis"], *signals)):
        profile = result["analysis"].get(scope) or {}
        signal = signals.get(scope) or {}
        signal_evidence = signal.get("evidence") or {}
        status = signal.get("status", "BLOCKED")
        reason = signal.get("reason")
        if status == "ERROR":
            state = "CALCULATION_ERROR"
        elif reason == "INSUFFICIENT_OBSERVATIONS":
            state = "INSUFFICIENT_HISTORICAL_SAMPLE"
        elif status != "COMPUTED":
            state = (
                reason
                if reason
                in (
                    "MISSING_INPUT",
                    "INVALID_VALUE",
                    "AMBIGUOUS_CANDIDATE",
                    "INCOMPATIBLE_CONTRACT",
                )
                else "MISSING_INPUT"
            )
            if state == "INCOMPATIBLE_CONTRACT":
                state = "INCOMPARABLE_CONTRACT"
        elif profile.get("is_tie"):
            state = "HISTORICAL_OUTCOME_TIE"
        else:
            state = "HISTORICAL_OUTCOME_READING"
        readings[scope] = interpretation_reading(
            state,
            status=status,
            reason=reason,
            comparability="COMPARABLE" if status == "COMPUTED" else "UNAVAILABLE",
            direction=profile.get("P5_DIRECTION"),
            strength=profile.get("P5_STRENGTH"),
            sample_size=(
                profile.get("sample_size")
                if profile.get("sample_size") is not None
                else signal_evidence.get("sample_size")
            ),
            bookmaker=profile.get("bookmaker") or signal_evidence.get("bookmaker"),
            bookie_id=profile.get("bookie_id") or signal_evidence.get("bookie_id"),
            market_group=profile.get("market_group")
            or signal_evidence.get("market_group"),
            market_period=profile.get("market_period")
            or signal_evidence.get("market_period"),
            wins_home=profile.get("wins_home"),
            wins_draw=profile.get("wins_draw"),
            wins_away=profile.get("wins_away"),
            evidence={
                "analysis_ref": f"analysis.{scope}",
                "signal_ref": scope,
                "sample_id": profile.get("sample_id")
                or signal_evidence.get("sample_id"),
            },
        )
    return {
        "version": INTERPRETATION_VERSION,
        "status": result["status"],
        "bookmakers": readings,
        "summary": "Lectura de resultados históricos por bookmaker; no es una probabilidad calibrada.",
    }


def build_p5_executive_summary(result, interpretation=None):
    """Select the existing full-time three-way memory reading per bookmaker."""
    interpretation = interpretation or result.get("INTERPRETATION") or interpret_p5_result(result)
    profiles = interpretation.get("bookmakers") or {}
    selected = {}
    for scope, reading in profiles.items():
        parts = scope.split(":")
        market_group = reading.get("market_group") or (parts[1] if len(parts) > 1 else None)
        market_period = reading.get("market_period") or (parts[2] if len(parts) > 2 else None)
        if market_group != "1X2" or str(market_period).casefold() not in {"full time", "ft"}:
            continue
        bookmaker = reading.get("bookmaker")
        if not bookmaker:
            continue
        selected[str(bookmaker).casefold()] = {
            "state": reading.get("state"),
            "sample_size": reading.get("sample_size") or 0,
            "home": reading.get("wins_home"),
            "draw": reading.get("wins_draw"),
            "away": reading.get("wins_away"),
            "direction": reading.get("direction"),
            "status": reading.get("status"),
            "reason": reading.get("reason"),
        }

    sentences = []
    for bookmaker, reading in selected.items():
        label = {
            "pinnacle": "Pinnacle",
            "bet365": "bet365",
            "sofascore": "SofaScore",
        }.get(bookmaker, bookmaker.capitalize())
        if reading["state"] == "INSUFFICIENT_HISTORICAL_SAMPLE":
            sentences.append(
                f"{label} no tiene muestra histórica suficiente "
                f"({reading['sample_size']} coincidencias)."
            )
        elif reading["state"] == "HISTORICAL_OUTCOME_TIE":
            sentences.append(
                f"{label} tiene {reading['sample_size']} coincidencias: "
                f"{reading['home']} HOME, {reading['draw']} DRAW y {reading['away']} AWAY; "
                f"empate de cuentas, dirección {reading['direction']}."
            )
        elif reading["state"] == "HISTORICAL_OUTCOME_READING":
            sentences.append(
                f"{label} tiene un resultado histórico dominante {reading['direction']} "
                f"en {reading['sample_size']} coincidencias."
            )
        elif reading["status"] == "BLOCKED":
            sentences.append(
                f"{label} no tiene una lectura disponible "
                f"({reading['reason'] or 'MISSING_INPUT'})."
            )

    return {
        "version": EXECUTIVE_SUMMARY_VERSION,
        "status": result.get("status"),
        "bookmakers": selected,
        "summary": " ".join(sentences)
        or "P5 no dispone de una muestra ejecutiva 1X2 de tiempo completo.",
    }


__all__ = [
    "build_p5_executive_summary",
    "MIN_SAMPLE_SIZE",
    "calculate_memory_profile",
    "interpret_p5_result",
    "msri_signal",
    "profile_strength",
    "sample_weight",
]
